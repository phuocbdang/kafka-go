package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"kafka-go/api"
	"kafka-go/cluster"
	"kafka-go/storage"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
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

	metadataMu sync.RWMutex
	metadata   map[string]string // map of node IDs to their public gRPC addresses

	raft *raft.Raft
}

// newServer creates a new gRPC server instance.
func newServer(dataDir string) (*server, error) {
	srv := &server{
		dataDir:    dataDir,
		logs:       make(map[string]*storage.CommitLog),
		offsets:    make(map[string]int64),
		offsetPath: filepath.Join(dataDir, "offsets.json"),
		metadata:   make(map[string]string),
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

	response, ok := applyFuture.Response().(cluster.ApplyResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected fsm response type")
	}

	return &api.ProduceResponse{
		Offset:    response.Offset,
		ErrorCode: api.ErrorCode_NONE,
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
	log.Printf("update metadata, nodeId %s, grpcAddr %s", nodeID, grpcAddr)
	s.metadata[nodeID] = grpcAddr
}

// setupRaft initializes and returns a Raft instance.
func setupRaft(nodeID, raftAddr, dataDir string, srv *server) (*raft.Raft, error) {
	config := raft.DefaultConfig()
	// Sets the unique identifier for this specific node within the Raft cluster.
	// The nodeID is the string we passed in from the command-line flags (e.g., "node1").
	// It's converted to the raft.ServerID type.
	// This ID must be unique across the entire cluster.
	config.LocalID = raft.ServerID(nodeID)

	// Ensure the provided network address is valid before trying to use it.
	addr, err := resolveAdvertisableAddr(raftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve advertiable address: %w", err)
	}

	// Creates the network layer that Raft nodes will use to communicate with each other.
	// It opens a TCP listener on raftAddress and configures it.
	// Raft is a distributed protocol, nodes must be able to send RPCs (like votes and log entries) to each other.
	// This transport handles all that low-level networking.
	transport, err := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft transport: %w", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Creates the persistent storage for the Raft log.
	// It uses the raft-boltdb library to create and manage a file named raft.db inside the node's data directory.
	// The log of commands agreed upon by the cluster must survive crashes and restarts.
	// This file stores that log durably on disk.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create bolt store: %w", err)
	}

	// Creates an instance of our custom Finite State Machine (FSM).
	// It passes the srv.GetOrCreateLog method to the FSM's constructor.
	// The FSM is the bridge between Raft's log and our application's state (the CommitLog files).
	// By passing this function, we give the FSM the ability to write message data to the correct file
	// when a Produce command is committed by the Raft cluster.
	fsm := cluster.NewFSM(srv)

	ra, err := raft.NewRaft(config, fsm, logStore, logStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	return ra, nil
}

// bootstrapCluster handles the logic of either starting a new cluster or joining an existing one.
func bootstrapCluster(nodeID, raftAddr, grpcAddr, joinAddr string, ra *raft.Raft) error {
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
		bootstrapFuture := ra.BootstrapCluster(configuration)
		if err := bootstrapFuture.Error(); err != nil {
			return fmt.Errorf("failed to bootstrap cluster: %w", err)
		}

		log.Println("waiting for the node to become the leader...")
		select {
		case <-ra.LeaderCh():
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
		applyFuture := ra.Apply(cmdBytes, 5*time.Second)
		if err := applyFuture.Error(); err != nil {
			return fmt.Errorf("failed to apply initial metadata: %w", err)
		}
		log.Println("bootstrapped cluster successfully")
		return nil
	}

	log.Printf("attempting to join existing cluster at %s", joinAddr)
	// joining might not succeed on the first try (e.g., the leader might be busy
	// or a network blip might occur)
	// The loop ensures the node is persistent and will keep retrying
	for {
		time.Sleep(1 * time.Second)
		url := fmt.Sprintf("http://%s/join?peerID=%s&peerRaftAddr=%s&peerGRPCAddr=%s",
			joinAddr, nodeID, raftAddr, grpcAddr)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("failed to join cluster, retrying: %v", err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			log.Println("successfully joined cluster")
			return nil
		}

		body, _ := io.ReadAll(resp.Body)
		log.Printf("failed to join cluster, status: %s, body: %s, retrying",
			resp.Status, body)
	}
}

func main() {
	nodeID := flag.String("id", "", "Node ID")
	grpcAddr := flag.String("grpc_addr", "127.0.0.1:9092", "Address for gRPC server")
	// raftAddr is the network address that the current broker node will listen on for all Raft-related communication.
	// Every node in the cluster must have one, and it must be reachable by all other nodes.
	raftAddr := flag.String("raft_addr", "127.0.0.1:19902", "Address for Raft server")
	// joinAddr is the network address of another, already existing node in the cluster.
	// A new node uses this address to find the existing cluster and ask to be added as a member.
	joinAddr := flag.String("join_addr", "", "Address to join an existing Raft cluster")
	httpAddr := flag.String("http_addr", "127.0.0.1:8080", "Address for HTTP join server")
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
	srv, err := newServer(dataDir)
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
	ra, err := setupRaft(*nodeID, *raftAddr, dataDir, srv)
	if err != nil {
		log.Fatalf("failed to setup raft: %v", err)
	}
	srv.raft = ra
	log.Println("finish setting up raft!")

	// --- Start HTTP server for joining ---
	http.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		peerID := r.URL.Query().Get("peerID")
		peerRaftAddr := r.URL.Query().Get("peerRaftAddr")
		peerGRPCAddr := r.URL.Query().Get("peerGRPCAddr")

		if ra.State() != raft.Leader {
			http.Error(w, "not the leader", http.StatusBadRequest)
			return
		}

		// Add node as a voter in Raft cluster
		addPeerFuture := ra.AddVoter(raft.ServerID(peerID), raft.ServerAddress(peerRaftAddr), 0, 5*time.Second)
		if err := addPeerFuture.Error(); err != nil {
			http.Error(w, fmt.Sprintf("failed to add peer: %s", err), http.StatusInternalServerError)
		}

		// replicate metadata
		payload := cluster.UpdateMetadataCommandPayload{
			NodeID:   peerID,
			GRPCAddr: peerGRPCAddr,
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			http.Error(w, "failed to marshal metadata payload", http.StatusInternalServerError)
			return
		}
		cmd := cluster.Command{
			Type:    cluster.UpdateMetadataCommand,
			Payload: payloadBytes,
		}
		cmdBytes, err := json.Marshal(cmd)
		if err != nil {
			http.Error(w, "failed to marshal metadata command", http.StatusInternalServerError)
			return
		}

		applyFuture := ra.Apply(cmdBytes, 5*time.Second)
		if err := applyFuture.Error(); err != nil {
			http.Error(w, fmt.Sprintf("failed to apply metadata command: %s", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
	log.Printf("join server listening on %s", *httpAddr)
	go func() {
		if err := http.ListenAndServe(*httpAddr, nil); err != nil {
			log.Fatalf("failed to server http: %v", err)
		}
	}()

	// --- Cluster Bootstrapping ---
	if err := bootstrapCluster(*nodeID, *raftAddr, *grpcAddr, *joinAddr, ra); err != nil {
		log.Fatalf("failed to bootstrap or join cluster: %v", err)
	}

	// Block forever
	select {}
}
