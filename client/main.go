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
	fmt.Println("  client produce <message>")
	fmt.Println("  client consume <offset>")
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

	case "consume":
		if len(os.Args) != 5 {
			printUsage()
			log.Fatal("consume command requires topic, partition and offset")
		}

		topic := os.Args[2]
		partition, err := strconv.ParseUint(os.Args[3], 10, 32)
		if err != nil {
			log.Fatalf("invalid partition: %v", err)
		}
		offset, err := strconv.ParseInt(os.Args[4], 10, 64)
		if err != nil {
			log.Fatalf("invalid offset: %v", err)
		}

		req := &api.ConsumeRequest{
			Topic:     topic,
			Partition: uint32(partition),
			Offset:    offset,
		}

		resp, err := client.Consume(ctx, req)
		if err != nil {
			log.Fatalf("could not consume: %v", err)
		}
		log.Printf("consumed message: '%s'", string(resp.Record.Value))
		log.Printf("next offset is: '%d'", resp.Record.Offset)

	default:
		log.Fatalf("unknown command: %s", command)
	}
}
