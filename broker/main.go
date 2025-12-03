package main

import (
	"context"
	"encoding/json"
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

	// Manage commit logs
	logMu sync.RWMutex
	logs  map[string]*storage.CommitLog

	// Manage consumer group offsets
	offsetMu   sync.RWMutex
	offsets    map[string]int64 // Key: <group_id>-<topic>-<partition>, Value: offset
	offsetPath string           // Path to the durable offsets file
}

// newServer creates a new gRPC server instance.
func newServer(dataDir string) (*server, error) {
	srv := &server{
		dataDir:    dataDir,
		logs:       make(map[string]*storage.CommitLog),
		offsets:    make(map[string]int64),
		offsetPath: filepath.Join(dataDir, "offsets.json"),
	}

	// Load offsets from the durable file on startup
	if err := srv.loadOffsets(); err != nil {
		return nil, fmt.Errorf("failed to load offsets: %w", err)
	}

	return srv, nil
}

// loadOffsets reads the offsets file from disks into the in-memory map
func (s *server) loadOffsets() error {
	data, err := os.ReadFile(s.offsetPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("offsets.json not found, starting with empty offsets.")
			return nil
		}
		return err
	}

	s.offsetMu.Lock()
	defer s.offsetMu.Unlock()

	return json.Unmarshal(data, &s.offsets)
}

// persistOffsets writes the current in-memory offset map to the Json file
func (s *server) persistOffsets() error {
	s.offsetMu.Lock()
	defer s.offsetMu.Unlock()

	data, err := json.Marshal(s.offsets)
	if err != nil {
		return err
	}

	return os.WriteFile(s.offsetPath, data, 0644)
}

// getOrCreateLog retrieves the commit log for a given topic and partition,
// creates the log and directories if not exists.
func (s *server) getOrCreateLog(topic string, partition uint32) (*storage.CommitLog, error) {
	logIdentifier := fmt.Sprintf("%s-%d", topic, partition)

	// Check a read lock for performance
	s.logMu.RLock()
	commitLog, ok := s.logs[logIdentifier]
	s.logMu.RUnlock()
	if ok {
		return commitLog, nil
	}

	// If commit log not found, acquire a write log to create
	s.logMu.Lock()
	defer s.logMu.Unlock()

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
	s.logMu.RLock()
	defer s.logMu.RUnlock()

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

// CommitOffset handles a request from a consumer to save its progress.
func (s *server) CommitOffset(ctx context.Context, req *api.CommitOffsetRequest) (*api.CommitOffsetResponse, error) {
	offsetIdentifier := fmt.Sprintf("%s-%s-%d", req.ConsumerGroupId, req.Topic, req.Partition)
	s.offsetMu.Lock()
	s.offsets[offsetIdentifier] = req.Offset
	s.offsetMu.Unlock()

	if err := s.persistOffsets(); err != nil {
		log.Printf("ERROR: failed to persist offsets: %v", err)
		return nil, fmt.Errorf("failed to commit offset")
	}

	return &api.CommitOffsetResponse{}, nil
}

// FetchOffset handles a request from a consumer to retrieve its last saved progress.
func (s *server) FetchOffset(ctx context.Context, req *api.FetchOffsetRequest) (*api.FetchOffsetResponse, error) {
	offsetIdentifier := fmt.Sprintf("%s-%s-%d", req.ConsumerGroupId, req.Topic, req.Partition)
	s.offsetMu.RLock()
	defer s.offsetMu.RUnlock()

	offset, ok := s.offsets[offsetIdentifier]
	if !ok {
		// If no offset is stored for this group, they start at the beginning.
		return &api.FetchOffsetResponse{Offset: 0}, nil
	}

	return &api.FetchOffsetResponse{Offset: offset}, nil
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
