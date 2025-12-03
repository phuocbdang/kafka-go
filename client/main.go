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

func handleProduce(ctx context.Context, client api.KafkaClient) {
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

	req := &api.ProduceRequest{
		Topic:     topic,
		Partition: uint32(partition),
		Value:     []byte(message),
	}

	resp, err := client.Produce(ctx, req)
	if err != nil {
		log.Fatalf("could not produce: %v", err)
	}
	log.Printf("message produced to offset: %d", resp.Offset)
}

func handleConsume(ctx context.Context, client api.KafkaClient) {
	if len(os.Args) != 5 {
		printUsage()
		log.Fatal("consume command requires group_id, topic, partition")
	}

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
	conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := api.NewKafkaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	command := os.Args[1]
	switch command {
	case "produce":
		handleProduce(ctx, client)
	case "consume":
		handleConsume(ctx, client)
	default:
		log.Fatalf("unknown command: %s", command)
	}
}
