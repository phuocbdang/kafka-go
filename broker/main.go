package main

import (
	"context"
	"flag"
	"fmt"
	"kafka-go/api"
	"kafka-go/storage"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/grpc"
)

// server implements the gRPC server for Kafka service.
type server struct {
	api.UnimplementedKafkaServer
	dataDir string
	mu      sync.RWMutex
	logs    map[string]*storage.CommitLog
}

// newServer creates a new gRPC server instance.
func newServer(dataDir string) (*server, error) {
	return &server{
		dataDir: dataDir,
		logs:    make(map[string]*storage.CommitLog),
	}, nil
}

// getOrCreateLog retrieves the commit log for a given topic and partition,
// creates the log and directories if not exists.
func (s *server) getOrCreateLog(topic string, partition uint32) (*storage.CommitLog, error) {
	logIdentifier := fmt.Sprintf("%s-%d", topic, partition)

	// Check a read lock for performance
	s.mu.RLock()
	commitLog, ok := s.logs[logIdentifier]
	s.mu.RUnlock()
	if ok {
		return commitLog, nil
	}

	// If commit log not found, acquire a write log to create
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-Checked Locking (DCL): Prevents race conditions by ensuring
	// only one 'CommitLog' is created per topic/partition, even if
	// multiple goroutines try to create it simultaneously.
	commitLog, ok = s.logs[logIdentifier]
	if ok {
		return commitLog, nil
	}

	// Create the log file path
	topicDir := filepath.Join(s.dataDir, topic)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create topic directory: %w", err)
	}
	logPath := filepath.Join(topicDir, fmt.Sprintf("%d.log", partition))

	// Create a new CommitLog instance
	commitLog, err := storage.NewCommitLog(logPath)
	if err != nil {
		return nil, fmt.Errorf("could not create commit log: %w", err)
	}

	// Store a new CommitLog instance
	s.logs[logIdentifier] = commitLog

	return commitLog, nil
}

// Produce handles a produce request from a client.
func (s *server) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	commitLog, err := s.getOrCreateLog(req.Topic, req.Partition)
	if err != nil {
		return nil, err
	}

	offset, err := commitLog.Append(req.Value)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{
		Offset: offset,
	}, nil
}

// getLog retrieves the commit log for a given topic and partition,
// returns an error if the log does not exist.
func (s *server) getLog(topic string, partition uint32) (*storage.CommitLog, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	logIdentifier := fmt.Sprintf("%s-%d", topic, partition)
	commitLog, ok := s.logs[logIdentifier]
	if !ok {
		return nil, fmt.Errorf("topic %s partition %d not found", topic, partition)
	}

	return commitLog, nil
}

// Consume handles a consume request from a client.
func (s *server) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	commitLog, err := s.getLog(req.Topic, req.Partition)
	if err != nil {
		return nil, err
	}

	record, nextOffset, err := commitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponse{
		Record: &api.Record{
			Value:  record,
			Offset: nextOffset,
		},
	}, nil
}

func main() {
	grpcAddr := flag.String("addr", "127.0.0.1:9092", "Address for gRPC server")
	dataDir := flag.String("data_dir", "./data", "Directory for logs")
	flag.Parse()

	log.Printf("starting kafka go broker on %s", *grpcAddr)

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	srv, err := newServer(*dataDir)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	api.RegisterKafkaServer(grpcServer, srv)

	log.Printf("broker listening on %s", *grpcAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
