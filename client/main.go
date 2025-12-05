package main

import (
	"context"
	"fmt"
	"kafka-go/api"
	"log"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  client produce <topic> <partition> <message>")
	fmt.Println("  client consume <group_id> <topic> <partition>")
}

func handleProduce(initialBrokerAddr string) {
	if len(os.Args) != 5 {
		printUsage()
		log.Fatal("produce command requires topic, partition and message")
	}

	topic := os.Args[2]
	partition, err := strconv.ParseUint(os.Args[3], 10, 32)
	if err != nil {
		log.Fatalf("invalid partition: %v", err)
	}
	message := os.Args[4]

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
			Value:     []byte(message),
		}

		resp, err := client.Produce(ctx, req)
		if err != nil {
			log.Fatalf("could not produce: %v", err)
		}

		if resp.ErrorCode == api.ErrorCode_NONE {
			log.Printf("message produced successfully (offset from leader is approximate)")
			return
		}

		if resp.ErrorCode == api.ErrorCode_NOT_LEADER {
			log.Printf("not the leader, leader is at %s. retrying ...", resp.LeaderAddr)
			currentBrokerAddr = resp.LeaderAddr
			time.Sleep(1 * time.Second)
			continue
		}
	}

	log.Fatal("failed to produce message after multiple retries")
}

func handleConsume(brokerAddr string) {
	if len(os.Args) != 5 {
		printUsage()
		log.Fatal("consume command requires group_id, topic, partition")
	}

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

	groupId := os.Args[2]
	topic := os.Args[3]
	partition, err := strconv.ParseUint(os.Args[4], 10, 32)
	if err != nil {
		log.Fatalf("invalid partition: %v", err)
	}

	//  Fetch the last committed offset for this consumer group
	fetchResp, err := client.FetchOffset(ctx, &api.FetchOffsetRequest{
		ConsumerGroupId: groupId,
		Topic:           topic,
		Partition:       uint32(partition),
	})
	if err != nil {
		log.Fatalf("could not fetch offset: %v", err)
	}

	currentOffset := fetchResp.Offset

	// Consume message
	consumeResp, err := client.Consume(ctx, &api.ConsumeRequest{
		Topic:     topic,
		Partition: uint32(partition),
		Offset:    currentOffset,
	})
	if err != nil {
		log.Fatalf("could not consume: %v", err)
	}
	log.Printf("consumed message: '%s'", string(consumeResp.Record.Value))
	log.Printf("next offset is: '%d'", consumeResp.Record.Offset)

	// Commit offset
	_, err = client.CommitOffset(ctx, &api.CommitOffsetRequest{
		ConsumerGroupId: groupId,
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
		os.Exit(1)
	}

	brokerAddr := "localhost:9092"

	command := os.Args[1]
	switch command {
	case "produce":
		handleProduce(brokerAddr)
	case "consume":
		handleConsume(brokerAddr)
	default:
		log.Fatalf("unknown command: %s", command)
	}
}
