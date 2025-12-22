package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka-go/api"
	"kafka-go/cluster"
	"kafka-go/storage"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	metadataMu sync.RWMutex
	metadata   map[string]string // map of node IDs to their public gRPC addresses

	coordinatorMu  sync.Mutex
	consumerGroups map[string]*ConsumerGroup // map of group id to its metadata.

	raft *raft.Raft
}

type ConsumerGroupState string

type ConsumerGroup struct {
	mu          sync.Mutex
	GroupID     string
	State       ConsumerGroupState
	Members     map[string]*GroupMember // consumer_id -> its metadata
	Assignments map[string][]uint32     // consumer_id -> partitions
	Topic       string
	Partitions  []uint32 // all available partition numbers for the topic

	generationID int
	joinTimer    *time.Timer
	waitingJoin  map[string]chan *api.JoinGroupResponse
	waitingSync  map[string]chan *api.SyncGroupResponse
}

// GroupMember represents a single consumer within a group.
type GroupMember struct {
	ConsumerID    string
	LastHeartbeat time.Time
}

// NewServer creates a new gRPC server instance.
func NewServer(dataDir string) (*server, error) {
	srv := &server{
		dataDir:        dataDir,
		logs:           make(map[string]*storage.CommitLog),
		offsets:        make(map[string]int64),
		offsetPath:     filepath.Join(dataDir, "offsets.json"),
		metadata:       make(map[string]string),
		consumerGroups: make(map[string]*ConsumerGroup),
	}

	go srv.heartbeatChecker()

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

// GetOrCreateLog retrieves the commit log for a given topic and partition,
// creates the log and directories if not exists.
func (s *server) GetOrCreateLog(topic string, partition uint32) (*storage.CommitLog, error) {
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
	// In Raft, all changes to the system's state (like producing a new message) must go through the leader.
	// This ensures that all changes are put into a single, globally agreed-upon order.
	// If a client mistakenly sends a Produce request to a follower, this code rejects the request
	// and helpfully provides the address of the current leader so the client can retry.
	if s.raft.State() != raft.Leader {
		_, leaderID := s.raft.LeaderWithID()
		s.metadataMu.RLock()
		leaderGRPCAddr, ok := s.metadata[string(leaderID)]
		s.metadataMu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("leader gRPC address not found in metadata")
		}

		return &api.ProduceResponse{
			ErrorCode:  api.ErrorCode_NOT_LEADER,
			LeaderAddr: leaderGRPCAddr,
		}, nil
	}

	payload := cluster.ProduceCommandPayload{
		Topic:     req.Topic,
		Partition: req.Partition,
		Value:     req.Value,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	cmd := cluster.Command{
		Type:    cluster.ProduceCommand,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	// 1. The leader writes the command to its own internal Raft log.
	// 2. It sends the command to all follower nodes.
	// 3. It waits until a majority of nodes in the cluster have safely saved the command to their logs.
	// 4. Only then does the applyFuture.Error() call return without an error.
	// This guarantees the message is durably replicated before we ever respond to the client.
	applyFuture := s.raft.Apply(cmdBytes, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		return nil, fmt.Errorf("failed to apply raft command: %w", err)
	}

	if req.Ack == api.AckLevel_NONE {
		// File-and-forget. Return immediately without waiting for Raft to commit.
		return &api.ProduceResponse{
			ErrorCode: api.ErrorCode_OK,
		}, nil
	}

	response, ok := applyFuture.Response().(cluster.ApplyResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected fsm response type")
	}

	return &api.ProduceResponse{
		Offset:    response.Offset,
		ErrorCode: api.ErrorCode_OK,
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

// Join handles a request from a new node to join the cluster.
func (s *server) Join(ctx context.Context, req *api.JoinRequest) (*api.JoinResponse, error) {
	if s.raft.State() != raft.Leader {
		_, leaderID := s.raft.LeaderWithID()
		if leaderID == "" {
			return nil, fmt.Errorf("not the leader, and no leader is known")
		}

		s.metadataMu.RLock()
		leaderGRPCAddr, ok := s.metadata[string(leaderID)]
		s.metadataMu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("not the leader, and leader metadata not yet available for leader ID: %s", leaderID)
		}
		return nil, fmt.Errorf("not the leader, current leader is at %s", leaderGRPCAddr)
	}

	// Tell the Raft consensus layer about the new node.
	// It adds the new node's ID and its private Raft address to the cluster's configuration.
	addPeerFuture := s.raft.AddVoter(raft.ServerID(req.NodeId), raft.ServerAddress(req.RaftAddr), 0, 5*time.Second)
	if err := addPeerFuture.Error(); err != nil {
		return nil, fmt.Errorf("failed to add peer as voter: %w", err)
	}

	payload := cluster.UpdateMetadataCommandPayload{
		NodeID:   req.NodeId,
		GRPCAddr: req.GrpcAddr,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata payload: %w", err)
	}
	cmd := cluster.Command{
		Type:    cluster.UpdateMetadataCommand,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("fail to marshal metadata command: %w", err)
	}

	// Tell the application layer on all nodes about the new node's public-facing gRPC address.
	applyFuture := s.raft.Apply(cmdBytes, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		return nil, fmt.Errorf("failed to apply metadata command: %w", err)
	}

	log.Printf("successfully added node %s to the cluster", req.NodeId)
	return &api.JoinResponse{}, nil
}

// resolveAdvertisableAddr resolves a potentially non-advertisable bind address
// into an advertisable address that can be shared with other nodes.
func resolveAdvertisableAddr(addr string) (net.Addr, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	// If the address is wildcard (0.0.0.0 or :port), we need to find a specific IP.
	if tcpAddr.IP.IsUnspecified() {
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			return nil, err
		}

		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPAddr); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					return &net.TCPAddr{
						IP:   ipnet.IP,
						Port: tcpAddr.Port,
					}, nil
				}
			}
		}
		// If no suitable non-loopback IP is found, default to localhost for local testing.
		return &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: tcpAddr.Port,
		}, nil
	}

	return tcpAddr, nil
}

// UpdateMetadata is called by the FSM to update the server's in-memory metadata map.
// This method makes the server struct satisfy the cluster.StateManager interface.
func (s *server) UpdateMetadata(nodeID, grpcAddr string) {
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	log.Printf("update metadata, nodeID %s, grpcAddr %s", nodeID, grpcAddr)
	s.metadata[nodeID] = grpcAddr
}

// SetupRaft initializes and returns a Raft instance.
func (s *server) SetupRaft(nodeID, raftAddr, dataDir string) error {
	config := raft.DefaultConfig()
	// Sets the unique identifier for this specific node within the Raft cluster.
	// The nodeID is the string we passed in from the command-line flags (e.g., "node1").
	// It's converted to the raft.ServerID type.
	// This ID must be unique across the entire cluster.
	config.LocalID = raft.ServerID(nodeID)

	// Ensure the provided network address is valid before trying to use it.
	addr, err := resolveAdvertisableAddr(raftAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve advertiable address: %w", err)
	}

	// Creates the network layer that Raft nodes will use to communicate with each other.
	// It opens a TCP listener on raftAddress and configures it.
	// Raft is a distributed protocol, nodes must be able to send RPCs (like votes and log entries) to each other.
	// This transport handles all that low-level networking.
	transport, err := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create raft transport: %w", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Creates the persistent storage for the Raft log.
	// It uses the raft-boltdb library to create and manage a file named raft.db inside the node's data directory.
	// The log of commands agreed upon by the cluster must survive crashes and restarts.
	// This file stores that log durably on disk.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("failed to create bolt store: %w", err)
	}

	// Creates an instance of our custom Finite State Machine (FSM).
	// The FSM is the bridge between Raft's log and our application's state (the CommitLog files).
	fsm := cluster.NewFSM(s)

	ra, err := raft.NewRaft(config, fsm, logStore, logStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("failed to create raft: %w", err)
	}
	s.raft = ra
	return nil
}

// BootstrapCluster handles the logic of either starting a new cluster or joining an existing one.
func (s *server) BootstrapCluster(nodeID, raftAddr, grpcAddr, joinAddr string) error {
	bootstrap := joinAddr == ""
	// bootstrap == true means we are going to start a new cluster.
	if bootstrap {
		log.Println("bootstrapping new cluster")
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(nodeID),
					Address: raft.ServerAddress(raftAddr),
				},
			},
		}
		bootstrapFuture := s.raft.BootstrapCluster(configuration)
		if err := bootstrapFuture.Error(); err != nil {
			return fmt.Errorf("failed to bootstrap cluster: %w", err)
		}

		log.Println("waiting for the node to become the leader...")
		select {
		case <-s.raft.LeaderCh():
			log.Println("node has become the leader.")
		case <-time.After(10 * time.Second):
			return fmt.Errorf("timed out waiting for leadership")
		}

		// replicate leader metadata to Raft log
		payload := cluster.UpdateMetadataCommandPayload{
			NodeID:   nodeID,
			GRPCAddr: grpcAddr,
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		cmd := cluster.Command{
			Type:    cluster.UpdateMetadataCommand,
			Payload: payloadBytes,
		}
		cmdBytes, err := json.Marshal(cmd)
		if err != nil {
			return err
		}
		applyFuture := s.raft.Apply(cmdBytes, 5*time.Second)
		if err := applyFuture.Error(); err != nil {
			return fmt.Errorf("failed to apply initial metadata: %w", err)
		}
		log.Println("bootstrapped cluster successfully")
		return nil
	}

	log.Printf("attempting to join existing cluster at %s", joinAddr)
	currentJoinAddr := joinAddr
	leaderAddrRegex := regexp.MustCompile(`([0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]+`)
	// Joining might not succeed on the first try (e.g., the leader might be busy
	// or a network blip might occur)
	// The loop ensures the node is persistent and will keep retrying
	for {
		time.Sleep(1 * time.Second)

		conn, err := grpc.NewClient(currentJoinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("failed to connect to join address, retrying: %v", err)
			continue
		}
		client := api.NewKafkaClient(conn)
		_, err = client.Join(context.Background(), &api.JoinRequest{
			NodeId:   nodeID,
			RaftAddr: raftAddr,
			GrpcAddr: grpcAddr,
		})
		conn.Close()
		if err == nil {
			log.Println("successfully joined cluster")
			return nil
		}

		if strings.Contains(err.Error(), "not the leader") {
			matches := leaderAddrRegex.FindStringSubmatch(err.Error())
			if len(matches) > 0 {
				newLeaderAddr := matches[0]
				log.Printf("join failed: target node is not the leader, redirected to new leader at %s", newLeaderAddr)
				currentJoinAddr = newLeaderAddr
				continue
			}
			log.Printf("join failed: target node is not the leader, but could not parse leader hint. retrying original address... (%v)", err)
			continue
		}
		log.Printf("failed to join cluster with address %s, retrying: %v", currentJoinAddr, err)
	}
}
