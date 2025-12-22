package main

import (
	"flag"
	"kafka-go/api"
	"kafka-go/broker"
	"log"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
)

func main() {
	nodeID := flag.String("id", "", "Node ID")
	// grpcAddr is the main address that clients (producers or consumers) connect to.
	grpcAddr := flag.String("grpc_addr", "127.0.0.1:9092", "Address for gRPC server")
	// raftAddr is the address that the broker nodes use to talk each other for all internal Raft consensus business.
	raftAddr := flag.String("raft_addr", "127.0.0.1:19902", "Address for Raft server")
	// joinAddr is the address of another, already existing node in the cluster.
	// A new node uses this address to find the existing cluster and ask to be added as a member.
	joinAddr := flag.String("join_addr", "", "Address to join an existing Raft cluster")
	dataDirRoot := flag.String("data_dir", "./data", "Directory for logs")
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("node id is required")
	}

	dataDir := filepath.Join(*dataDirRoot, *nodeID)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}

	// --- Start gRPC Server ---
	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	srv, err := broker.NewServer(dataDir)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	api.RegisterKafkaServer(grpcServer, srv)

	log.Printf("broker listening on %s", *grpcAddr)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// --- Setup Raft ---
	log.Println("start setting up raft...")
	err = srv.SetupRaft(*nodeID, *raftAddr, dataDir)
	if err != nil {
		log.Fatalf("failed to setup raft: %v", err)
	}
	log.Println("finish setting up raft!")

	// --- Cluster Bootstrapping ---
	log.Println("starting cluster bootstrap/join process...")
	if err := srv.BootstrapCluster(*nodeID, *raftAddr, *grpcAddr, *joinAddr); err != nil {
		log.Fatalf("failed to bootstrap or join cluster: %v", err)
	}

	// Block forever
	select {}
}
