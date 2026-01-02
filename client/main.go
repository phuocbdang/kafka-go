package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"kafka-go/api"
	"log"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func printUsage() {
	fmt.Println("Usage: client <command> [args]")
	fmt.Println("")
	fmt.Println("Commands:")
	fmt.Println("  create-topic: Creates a new topic in the cluster.")
	fmt.Println("    --bootstrap-server <addr>		 Required. A broker address to connect to.")
	fmt.Println("    --topic <name>					 Required. The name of the new topic.")
	fmt.Println("    --partitions <num>				 Optional. The number of partitions. Default: 1.")
	fmt.Println(" 	produce: Produces a message to a topic partition.")
	fmt.Println("    --bootstrap-server <addr>		 Required. A broker address to connect to.")
	fmt.Println("		--topic-- <topic>			 Required. The topic to produce to.")
	fmt.Println("		--acks <level>				 Optional. Acknowledgment level (none, all). Default: all.")
	fmt.Println("		<partition> <value>		   	 Required. The partition number and the message value.")
	fmt.Println("	consume: Consumes messages from a topic as part of a consumer group.")
	fmt.Println("		--bootstrap-server <addr>	 Required. A broker address to connect to.")
	fmt.Println("		--topic <topic>				 Required. The topic to consume from.")
	fmt.Println("		--group <group_id>			 Required. The consumer group ID.")
}

func handleCreateTopic(brokerAddr string, topic string, partitions uint32) {
	conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := api.NewKafkaClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &api.CreateTopicRequest{
		Topic:      topic,
		Partitions: partitions,
	}

	resp, err := client.CreateTopic(ctx, req)
	if err != nil {
		log.Fatalf("could not create topic: %v", err)
	}

	if resp.ErrorCode == api.ErrorCode_TOPIC_ALREADY_EXISTS {
		log.Printf("topic %s already exists", topic)
		return
	}

	if resp.ErrorCode == api.ErrorCode_OK {
		log.Printf("topic '%s' created successfully with %d partitions", topic, partitions)
		return
	}

	log.Printf("received unexpected error code: %s", resp.ErrorCode)
}

func handleProduce(brokerAddrs string, topic string, partition uint32, value string, acks string) {
	var ackLevel api.AckLevel
	switch strings.ToLower(acks) {
	case "none":
		ackLevel = api.AckLevel_NONE
	case "all":
		ackLevel = api.AckLevel_ALL
	default:
		log.Fatalf("invalid acks level (none, all): %s", acks)
	}

	state.mu.Lock()
	state.sequenceNumber++
	currentSequence := state.sequenceNumber
	state.mu.Unlock()

	bootstrapServers := strings.Split(brokerAddrs, ",")
	if len(bootstrapServers) == 0 || bootstrapServers[0] == "" {
		log.Fatal("no bootstrap servers provided")
	}

	currentBrokerAddr := bootstrapServers[0]
	serverIndex := 0

	for range 10 {
		log.Printf("attempting to produce to broker at %s (seq: %d)", currentBrokerAddr, currentSequence)

		conn, err := grpc.NewClient(currentBrokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("failed to connect to %s: %v. failing over to next broker.", currentBrokerAddr, err)
			serverIndex = (serverIndex + 1) % len(bootstrapServers)
			currentBrokerAddr = bootstrapServers[serverIndex]
			time.Sleep(1 * time.Second)
			continue
		}

		client := api.NewKafkaClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		req := &api.ProduceRequest{
			Topic:          topic,
			Partition:      uint32(partition),
			Value:          []byte(value),
			Ack:            ackLevel,
			ProducerId:     state.producerID,
			SequenceNumber: currentSequence,
		}

		resp, err := client.Produce(ctx, req)
		conn.Close()
		cancel()

		if err != nil {
			log.Fatalf("could not produce: %v. failing over to next broker.", err)
			serverIndex = (serverIndex + 1) % len(bootstrapServers)
			currentBrokerAddr = bootstrapServers[serverIndex]
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.ErrorCode == api.ErrorCode_NOT_LEADER {
			log.Printf("not the leader, leader is at %s. retrying ...", resp.LeaderAddr)
			currentBrokerAddr = resp.LeaderAddr
			time.Sleep(1 * time.Second)
			continue
		}

		if ackLevel == api.AckLevel_ALL {
			log.Printf("message produced successfully to offset: %d", resp.Offset)
		} else {
			log.Printf("message sent successfully (fire-and-forget)")
		}
		return
	}

	log.Fatal("failed to produce message after multiple retries")
}

// Consumer manages the state of a single consumer instance.
type Consumer struct {
	BrokerAddrs []string
	GroupID     string
	Topic       string
	ConsumerID  string

	conn   *grpc.ClientConn
	client api.KafkaClient

	partitions []uint32 // The partitions currently assigned to this consumer
}

// Run starts the consumer's main loop. It will run forever, re-joining
// the group whenever a rebalance is triggered.
func (c *Consumer) Run() {
	log.Printf("consumer %s starting for group %s and topic %s", c.ConsumerID, c.GroupID, c.Topic)

	// Main rebalance loop - resiliency loop
	// I. Happy case (Stable Consumption):
	// 1. The for loop in Run() starts its first iteration.
	// 2. The client connects and successfully calls joinAndSync().
	// It now has an assignment of partitions (e.g., [0, 2]).
	// 3. It starts the heartbeatLoop as a background task. This goroutine will now run independently,
	// sending "I'm alive" signals to the broker every few seconds.
	// 4. It then calls c.consumeLoop(ctx): internal for loop that runs forever, fetching and processing messages.
	// The Run() function's main loop is now blocked and effectively paused on this line,
	// waiting for consumeLoop to finish.
	//
	// II. The Rebalance Event (The Loop Repeats)
	// Now, let's say another consumer joins the group.
	// 1. The Signal: The broker detects the new consumer and needs to trigger a rebalance.
	// It does this by sending a REBALANCE_IN_PROGRESS error back to our client's next Heartbeat request.
	// 2. The Internal Trigger: The heartbeatLoop receives this error. It immediately calls the cancel() function.
	// 3. Stopping Work: The consumeLoop, which is busy processing messages,
	// is also constantly listening for this cancellation signal (case <- ctx.Done()).
	// It sees the signal, stops its internal loop, and returns.
	// 4. The Loop Proceeds: The Run() function is now unblocked. Execution continues past the c.consumeLoop(ctx) line.
	// 5. Cleanup: The code performs cleanup (cancel(), <-heartbeatDone, disconnect()).
	// 6. Repeat: The for loop statement causes execution to jump back to the top. The client logs Rebalance triggered,
	// re-joining group... and begins the next iteration of the loop.
	// It will now call joinAndSync() again to get a new assignment.
	for {
		err := c.connectToCoordinator()
		if err != nil {
			log.Printf("failed to connect to any broker, retrying in 5s: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		err = c.joinAndSync()
		if err != nil {
			log.Printf("failed to join/sync group, will retry: %v", err)
			c.disconnect()
			time.Sleep(2 * time.Second)
			return
		}

		// We have a new assignment, start heartbeating and consuming
		ctx, cancel := context.WithCancel(context.Background())
		// heartbeatDone is to allow the main Run() goroutine to safety wait for the background heartbeatLoop goroutine
		// to finish its work and exit cleanly.
		// Flow:
		// 1. The Signal: A rebalance is triggered. The heartbeatLoop (running in the background) detects this and calls
		// cancel(). This tells the consumeLoop (running in the foreground) to stop.
		// 2. The Wait: the consumeLoop stops, and execution returns to the main Run() loop.
		// The Run() loop then immediately hits this line: `<-heartbeatDone`
		// At this moment, the heartbeatLoop might still be in the process of shutting down.
		// The Run() loop pauses here, waiting for a signal on the heartbeatDone channel.
		// 3. The Confirmation: The heartbeatLoop is designed to send a signal just before it exits (defer close(done))
		// 4. Processing Safety: As soon as the channel is closed, the <-heartbeatDone line in the Run() loop unblocks.
		// The Run() loop now knows with 100% certainty that the heartbeatLoop goroutine has exited. It can then safety
		// processed to the next step, c.disconnect(), without worrying about a race condition (like closing a network
		// connection that the heartbeat loop might still be using).
		heartbeatDone := make(chan struct{})

		go c.heartbeatLoop(ctx, cancel, heartbeatDone)

		c.consumeLoop(ctx)

		// A rebalance was triggered. Stop the heartbeat and consume loops.
		cancel()
		<-heartbeatDone // Wait for the heartbeat goroutine exit
		c.disconnect()
		log.Printf("rebalance triggered, re-joining group...")
	}
}

// connectToCoordinator tries to connect to one of the brokers.
func (c *Consumer) connectToCoordinator() error {
	for _, addr := range c.BrokerAddrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			c.conn = conn
			c.client = api.NewKafkaClient(conn)
			log.Printf("connected to broker at %s", addr)
			return nil
		}
	}
	return fmt.Errorf("could not connect to any of provided brokers")
}

// disconnect closes the gRPC connection.
func (c *Consumer) disconnect() {
	if c.conn != nil {
		c.conn.Close()
	}
}

// joinAndSync performs the two-phase join/sync protocol.
func (c *Consumer) joinAndSync() error {
	// Phase 1: JoinGroup
	log.Println("sending join group request...")
	joinResp, err := c.client.JoinGroup(context.Background(), &api.JoinGroupRequest{
		GroupId:    c.GroupID,
		ConsumerId: c.ConsumerID,
		Topic:      c.Topic,
	})
	if err != nil {
		return fmt.Errorf("join group failed: %w", err)
	}

	// Phase 2: SyncGroup
	var assignments map[string]*api.PartitionAssignment
	if joinResp.IsLeader {
		log.Println("elected as group leader. assigning partitions.")
		assignments = c.assignPartitions(joinResp.Members, joinResp.Partitions)
	}

	log.Println("sending sync group request...")
	syncResp, err := c.client.SyncGroup(context.Background(), &api.SyncGroupRequest{
		GroupId:     c.GroupID,
		ConsumerId:  c.ConsumerID,
		Assignments: assignments, // Will be nil if this consumer is not the leader
	})
	if err != nil {
		return fmt.Errorf("sync group failed: %w", err)
	}

	c.partitions = syncResp.Assignment.Partitions
	log.Printf("sync resp assignment for %s: %v", c.ConsumerID, c.partitions)
	log.Printf("successfully joined group. assigned partitions: %v", c.partitions)
	return nil
}

// assignPartitions implements a simple round-robin assignment strategy.
func (c *Consumer) assignPartitions(members map[string]*api.TopicMetadata, partitions []uint32) map[string]*api.PartitionAssignment {
	assignments := make(map[string]*api.PartitionAssignment)
	memberIDs := make([]string, 0, len(members))
	for id := range members {
		memberIDs = append(memberIDs, id)
		assignments[id] = &api.PartitionAssignment{}
	}

	for i, p := range partitions {
		memberIndex := i % len(memberIDs)
		memberID := memberIDs[memberIndex]
		assignments[memberID].Partitions = append(assignments[memberID].Partitions, p)
	}
	return assignments
}

// heartbeatLoop sends periodic heartbeat to the coordinator. It cancels the main
// context if a rebalance is triggered.
func (c *Consumer) heartbeatLoop(ctx context.Context, cancel context.CancelFunc, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := c.client.Heartbeat(ctx, &api.HeartbeatRequest{
				GroupId:    c.GroupID,
				ConsumerId: c.ConsumerID,
			})
			if err != nil || resp.ErrorCode == api.ErrorCode_REBALANCE_IN_PROGRESS {
				log.Printf("heartbeat failed or coordinator trigger rebalance. stopping loops.")
				cancel() // Signal the consume loop to stop
				return
			}
		}
	}
}

// consumeLoop launches a goroutine for each assigned partition.
func (c *Consumer) consumeLoop(ctx context.Context) {
	if len(c.partitions) == 0 {
		log.Println("no partitions assigned. waiting for next rebalance.")
		<-ctx.Done() // Wait until a rebalance is triggered
		return
	}

	var wg sync.WaitGroup
	for _, partition := range c.partitions {
		wg.Add(1)
		go c.consumePartition(ctx, partition, &wg)
	}
	wg.Wait()
}

// consumePartition is a new worker function that handles a single partition.
func (c *Consumer) consumePartition(ctx context.Context, partition uint32, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("starting to consume from assigned partition %d", partition)

	fetchResp, err := c.client.FetchOffset(ctx, &api.FetchOffsetRequest{
		ConsumerGroupId: c.GroupID,
		Topic:           c.Topic,
		Partition:       partition,
	})
	if err != nil {
		log.Printf("failed to fetch offset for partition %d: %v", partition, err)
		return
	}
	offset := fetchResp.Offset

	for {
		select {
		case <-ctx.Done(): // Check if a rebalance has been triggered
			log.Printf("stopping consumption for partition %d", partition)
			return
		default:
			resp, err := c.client.Consume(ctx, &api.ConsumeRequest{
				Topic:     c.Topic,
				Partition: partition,
				Offset:    offset,
			})
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					// Retry the consume call
					log.Printf("partition %d not found, waiting for it to be created...", partition)
					time.Sleep(3 * time.Second)
					continue
				}

				if strings.Contains(err.Error(), "offset out of bounds") {
					// Wait for more messages
					time.Sleep(1 * time.Second)
					continue
				}

				log.Printf("consume failed for partition %d: %v", partition, err)
				return
			}

			log.Printf("partition %d consume: %s", partition, string(resp.Record.Value))
			offset = resp.Record.Offset

			_, err = c.client.CommitOffset(ctx, &api.CommitOffsetRequest{
				ConsumerGroupId: c.GroupID,
				Topic:           c.Topic,
				Partition:       partition,
				Offset:          offset,
			})
			if err != nil {
				log.Printf("failed to commit offset for partition %d: %v", partition, err)
				return
			}
		}
	}
}

type producerState struct {
	mu             sync.Mutex
	producerID     uint64
	sequenceNumber int64
}

var state producerState

func main() {
	pid, err := rand.Int(rand.Reader, new(big.Int).SetUint64(^uint64(0)))
	if err != nil {
		log.Fatalf("failed to generate producer ID: %v", err)
	}
	state = producerState{
		producerID:     pid.Uint64(),
		sequenceNumber: 0,
	}
	log.Printf("client started with Producer ID: %d", state.producerID)

	if len(os.Args) < 2 {
		printUsage()
		return
	}

	switch os.Args[1] {
	case "create-topic":
		createTopicCmd := flag.NewFlagSet("create-topic", flag.ExitOnError)
		bootstrapServer := createTopicCmd.String("bootstrap-server", "", "Required. Broker address")
		topic := createTopicCmd.String("topic", "", "Required. The name of the topic to create")
		partitions := createTopicCmd.Uint("partitions", 1, "The number of partitions for the new topic")

		createTopicCmd.Parse(os.Args[2:])

		if *bootstrapServer == "" || *topic == "" {
			printUsage()
			return
		}

		handleCreateTopic(*bootstrapServer, *topic, uint32(*partitions))
	case "produce":
		produceCmd := flag.NewFlagSet("produce", flag.ExitOnError)

		bootstrapServer := produceCmd.String("bootstrap-server", "", "Required. Comma-separated list of broker addresses.")
		topic := produceCmd.String("topic", "", "Required. The topic to produce to.")
		acks := produceCmd.String("acks", "all", "Acknowledgment level (none, all)")

		produceCmd.Parse(os.Args[2:])

		if *bootstrapServer == "" || *topic == "" || produceCmd.NArg() != 2 {
			printUsage()
			return
		}

		partition, err := strconv.ParseUint(produceCmd.Arg(0), 10, 32)
		if err != nil {
			log.Fatalf("invalid partition: %v", err)
		}

		value := produceCmd.Arg(3)

		handleProduce(*bootstrapServer, *topic, uint32(partition), value, *acks)
	case "consume":
		consumeCmd := flag.NewFlagSet("consume", flag.ExitOnError)
		bootstrapServer := consumeCmd.String("bootstrap-server", "", "Required. Comma-separated list of broker addresses.")
		topic := consumeCmd.String("topic", "", "Required. The topic to consume from.")
		group := consumeCmd.String("group", "", "Required. The consumer group ID.")

		if err := consumeCmd.Parse(os.Args[2:]); err != nil {
			log.Fatalf("error parsing consume command: %v", err)
			return
		}

		if *bootstrapServer == "" || *topic == "" || *group == "" {
			printUsage()
			return
		}

		consumer := &Consumer{
			BrokerAddrs: strings.Split(*bootstrapServer, ","),
			GroupID:     *group,
			Topic:       *topic,
			ConsumerID:  fmt.Sprintf("consumer-%s", uuid.New().String()),
		}

		consumer.Run()
	default:
		printUsage()
	}
}
