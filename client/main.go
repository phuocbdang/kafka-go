package main

import (
	"context"
	"flag"
	"fmt"
	"kafka-go/api"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func printUsage() {
	fmt.Println("Usage: client <command> [args]")
	fmt.Println("Commands:")
	fmt.Println("	produce [--args=<level>] <broker_addr> <topic> <partition> <value>")
	fmt.Println("	consume <broker_addr> <group_id> <topic> <partition>")
}

func handleProduce(initialBrokerAddr string, topic string, partition uint32, value string, acks string) {
	var ackLevel api.AckLevel
	switch strings.ToLower(acks) {
	case "none":
		ackLevel = api.AckLevel_NONE
	case "all":
		ackLevel = api.AckLevel_ALL
	default:
		log.Fatalf("invalid acks level (none, all): %s", acks)
	}

	currentBrokerAddr := initialBrokerAddr

	for i := 0; i < 5; i++ {
		log.Printf("attempting to produce to broker at %s", currentBrokerAddr)
		conn, err := grpc.NewClient(currentBrokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("did not connect: %v", err)
		}
		defer conn.Close()

		client := api.NewKafkaClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		req := &api.ProduceRequest{
			Topic:     topic,
			Partition: uint32(partition),
			Value:     []byte(value),
			Ack:       ackLevel,
		}

		resp, err := client.Produce(ctx, req)
		if err != nil {
			log.Fatalf("could not produce: %v", err)
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

func handleConsume(brokerAddr string, groupID string, topic string, partition uint32) {
	// Consume logic does not need leader redirection for this implementation
	// as any node can serve reads.
	conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := api.NewKafkaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	//  Fetch the last committed offset for this consumer group
	fetchResp, err := client.FetchOffset(ctx, &api.FetchOffsetRequest{
		ConsumerGroupId: groupID,
		Topic:           topic,
		Partition:       partition,
	})
	if err != nil {
		log.Fatalf("could not fetch offset: %v", err)
	}

	// Consume message
	consumeResp, err := client.Consume(ctx, &api.ConsumeRequest{
		Topic:     topic,
		Partition: uint32(partition),
		Offset:    fetchResp.Offset,
	})
	if err != nil {
		log.Fatalf("could not consume: %v", err)
	}
	log.Printf("consumed message: '%s'", string(consumeResp.Record.Value))
	log.Printf("next offset is: '%d'", consumeResp.Record.Offset)

	// Commit offset
	_, err = client.CommitOffset(ctx, &api.CommitOffsetRequest{
		ConsumerGroupId: groupID,
		Topic:           topic,
		Partition:       uint32(partition),
		Offset:          consumeResp.Record.Offset,
	})
	if err != nil {
		log.Fatalf("could not commit offset: %v", err)
	}
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		return
	}

	produceCmd := flag.NewFlagSet("produce", flag.ExitOnError)
	produceAcks := produceCmd.String("acks", "all", "Acknowledgment level (none, all)")

	consumeCmd := flag.NewFlagSet("consume", flag.ExitOnError)

	switch os.Args[1] {
	case "produce":
		if err := produceCmd.Parse(os.Args[2:]); err != nil {
			log.Fatalf("error parsing produce command: %v", err)
		}
		if produceCmd.NArg() != 4 {
			fmt.Println("Usage: client produce [--acks=<level>] <broker_addr> <topic> <partition> <value>")
			return
		}
		brokerAddr := produceCmd.Arg(0)
		topic := produceCmd.Arg(1)
		partition, err := strconv.ParseUint(produceCmd.Arg(2), 10, 32)
		if err != nil {
			log.Fatalf("invalid partition: %v", err)
		}
		value := produceCmd.Arg(3)
		handleProduce(brokerAddr, topic, uint32(partition), value, *produceAcks)
	case "consume":
		if err := consumeCmd.Parse(os.Args[2:]); err != nil {
			log.Fatalf("error parsing consume command: %v", err)
		}
		if consumeCmd.NArg() != 4 {
			fmt.Println("Usage: client consume <broker_addr> <group_id> <topic> <partition>")
			return
		}
		brokerAddr := consumeCmd.Arg(0)
		groupID := consumeCmd.Arg(1)
		topic := consumeCmd.Arg(2)
		partition, err := strconv.ParseUint(consumeCmd.Arg(3), 10, 32)
		if err != nil {
			log.Fatalf("invalid partition: %v", err)
		}
		handleConsume(brokerAddr, groupID, topic, uint32(partition))
	default:
		printUsage()
	}
}
