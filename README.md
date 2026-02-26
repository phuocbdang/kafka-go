# kafka-go

A distributed message broker built from scratch in Go — inspired by Apache Kafka's core architecture. Built as a deep-dive learning project to understand how Kafka actually works under the hood.

This project reimplements the fundamental internals of Kafka (distributed log, consumer groups, rebalancing, idempotent producers) using modern Go primitives and the Raft consensus algorithm.

---

## Overview

This project is a from-scratch implementation of the core ideas behind Apache Kafka:

- **Multiple broker nodes** form a cluster using the **Raft consensus algorithm** for leader election and log replication.
- Producers send messages to the **leader broker**, which replicates them to followers before acknowledging.
- Messages are stored in an **append-only commit log** backed by **memory-mapped files** for zero-copy I/O.
- Consumers join **consumer groups**. The broker acts as a group coordinator, running a **JoinGroup/SyncGroup protocol** (identical to real Kafka) to assign topic partitions fairly across consumers.
- Consumers send **periodic heartbeats** so the broker can detect crashes and trigger partition **rebalancing**.
- Producers are **idempotent** — duplicate or retried messages are safely deduplicated.

All communication between clients and brokers is via **gRPC** using a protobuf-defined API.

---

## Architecture Overview

```
                        ┌──────────────────────────────────────────────┐
                        │               Kafka Cluster                  │
                        │                                              │
  Producer/Consumer     │   ┌─────────────┐      ┌─────────────┐       │
  ─────────────────►    │   │   Node 1    │◄────►│   Node 2    │       │
       gRPC             │   │  (Leader)   │      │  (Follower) │       │
                        │   └──────┬──────┘      └─────────────┘       │
                        │          │  Raft                             │
                        │          │  Consensus                        │
                        │   ┌──────▼──────┐                            │
                        │   │   Node 3    │                            │
                        │   │  (Follower) │                            │
                        │   └─────────────┘                            │
                        └──────────────────────────────────────────────┘

  Each node:
  ┌─────────────────────────────────────┐
  │     gRPC Server (client-facing)     │
  │   ┌────────────────────────────┐    │
  │   │  Broker Logic              │    │
  │   │  - Produce / Consume       │    │
  │   │  - Consumer Group Coord.   │    │
  │   │  - Topic Metadata          │    │
  │   └────────────┬───────────────┘    │
  │                │                    │
  │   ┌────────────▼───────────────┐    │
  │   │  Raft FSM                  │    │
  │   │  (applies committed logs)  │    │
  │   └────────────┬───────────────┘    │
  │                │                    │
  │   ┌────────────▼───────────────┐    │
  │   │  CommitLog (mmap'd file)   │    │
  │   │  ./data/<node>/<topic>/    │    │
  │   │    0.log, 1.log, ...       │    │
  │   └────────────────────────────┘    │
  │                                     │
  │  Raft TCP Transport (node-to-node)  │
  └─────────────────────────────────────┘
```

---

## Features

| Feature | Description |
|---|---|
| **Raft Consensus** | Leader election and log replication via `hashicorp/raft` + BoltDB |
| **gRPC API** | All client-broker and broker-internal communication over gRPC |
| **Commit Log** | Append-only, offset-based log file per topic/partition |
| **mmap Zero-Copy** | Log files are memory-mapped for high-throughput reads/writes |
| **Idempotent Producer** | Deduplication using `(producerID, sequenceNumber)` pairs |
| **Topic Creation** | Topics with configurable partition count, replicated via Raft |
| **Consumer Groups** | Full JoinGroup → SyncGroup two-phase rebalancing protocol |
| **Round-Robin Assignment** | Consumer leader assigns partitions round-robin across members |
| **Heartbeat & Session Timeout** | Dead consumers are evicted and rebalance is auto-triggered |
| **Offset Management** | Per-group offsets committed to disk and fetched on reconnect |
| **Leader Redirect** | Followers redirect clients to the current Raft leader |
| **Dynamic Cluster Join** | New nodes join a running cluster via any existing node's address |
| **Ack Levels** | `NONE` (fire-and-forget) or `ALL` (wait for quorum commit) |

---

## How It Works — Deep Dives

### 1. Distributed Consensus with Raft

Every state-changing operation (produce, create topic, update node metadata) goes through the Raft leader. The leader writes the command to the Raft log, replicates it to a majority of nodes, and only then applies it.

```
Client → Leader gRPC → raft.Apply(cmd) → majority ACK → FSM.Apply() → CommitLog.Append()
```

The **Finite State Machine (FSM)** in `cluster/fsm.go` is the bridge. The Raft library guarantees that every node's FSM receives the exact same sequence of commands, so all nodes stay in sync automatically.

Three command types flow through Raft:
- `PRODUCE` — write a message to a topic/partition log
- `CREATE_TOPIC` — register topic metadata across all nodes
- `UPDATE_METADATA` — broadcast a node's gRPC address to the cluster

If a follower receives a client request, it reads the current leader's gRPC address from its in-memory metadata map and returns it in the response — the client then retries directly against the leader.

### 2. Commit Log Storage with mmap (Zero-Copy)

Each topic/partition is backed by a file (e.g., `data/node1/my-topic/0.log`). Instead of using `os.Write()` / `os.Read()` system calls, the file is **memory-mapped** using `unix.Mmap`.

```
Write:  binary.BigEndian.PutUint64(mmapData[pos:], recordLen)  // 8-byte length header
        copy(mmapData[pos+8:], data)                            // actual payload

Read:   recordLen = binary.BigEndian.Uint64(mmapData[offset:]) // read header
        copy(out, mmapData[offset+8 : offset+8+recordLen])     // read payload
```

**Why mmap?** The OS handles caching and flushing. Writes go directly to the page cache without extra kernel-to-userspace copies, which is the same technique the real Kafka uses for its log segments. The mmap region grows dynamically (doubling, up to 1 GB) as data is written.

Each record is prefixed with an 8-byte length field. The `offset` returned to clients is the **byte position** in the file, so reads are O(1) direct seeks.

### 3. Idempotent Producer

To prevent duplicate messages from network retries, each producer has a unique `producerID` (random uint64) and a monotonically increasing `sequenceNumber` per message.

The broker's FSM tracks the last sequence number seen per producer:

```go
// In AppendIdempotent:
lastSeq, ok := c.state.ProducerLastSequence[producerID]
if ok && sequenceNumber <= lastSeq {
    return -1, nil  // Duplicate — silently skip
}
```

This state is persisted to a `.state` sidecar file alongside each `.log`, so it survives broker restarts.

Additionally, the FSM checks the Raft log index against the last applied index before writing, so messages are never double-applied even if Raft replays entries after a crash:

```go
if logEntry.Index <= commitLog.GetLastAppliedIndex() {
    // Already applied — skip
    return ApplyResponse{Offset: -1}
}
```

### 4. Consumer Groups & Rebalancing

This is the most complex part — a faithful reimplementation of Kafka's group coordination protocol.

**The Two-Phase Protocol:**

```
Consumer                        Broker (Group Coordinator)
   │                                       │
   │──── JoinGroup(groupID, topic) ───────►│  Adds member, starts rebalance timer
   │                                       │  (waits for all members to join)
   │◄─── JoinGroupResponse ────────────────│  Leader gets full member list + partitions
   │     (isLeader=true for one consumer)  │  Others get just the leaderID
   │                                       │
   │  [Leader computes round-robin plan]   │
   │                                       │
   │──── SyncGroup(assignments) ──────────►│  Leader sends the full plan
   │──── SyncGroup(no assignments) ───────►│  Followers send empty
   │                                       │  (waits for ALL members to sync)
   │◄─── SyncGroupResponse ────────────────│  Each consumer gets its own partition slice
   │     (partitions=[0,2])                │
```

**Heartbeat & Session Timeout:**

Consumers send a heartbeat every 2 seconds. A background goroutine on the broker checks all groups every 2 seconds. If a member's last heartbeat is older than 10 seconds, it is evicted and a new rebalance is triggered. During an active rebalance, the checker skips the group entirely to avoid cascading rebalances.

**Client-Side Rebalance Loop:**

The consumer client runs an outer `for` loop in `Run()`. The heartbeat goroutine and the consume goroutine share a `context.Context`. When the heartbeat receives `REBALANCE_IN_PROGRESS`, it calls `cancel()`, which signals the consume loop to stop. Then the outer loop re-runs `joinAndSync()` to get a fresh assignment.

### 5. Leader Redirection

Any RPC that modifies state (Produce, CreateTopic, JoinGroup, SyncGroup, Heartbeat) checks `raft.State() != raft.Leader`. If it's a follower, it looks up the leader's gRPC address from the in-memory metadata map and returns it to the client:

```go
if s.raft.State() != raft.Leader {
    _, leaderID := s.raft.LeaderWithID()
    leaderGRPCAddr := s.metadata[string(leaderID)]
    return &api.ProduceResponse{
        ErrorCode:  api.ErrorCode_NOT_LEADER,
        LeaderAddr: leaderGRPCAddr,
    }, nil
}
```

The client retries against the returned address. This gives the cluster transparent failover — clients don't need to know which node is the leader ahead of time.

---

## Project Structure

```
kafka-go/
├── cmd/
│   └── main.go          # Broker entry point — flags, gRPC server, Raft setup
├── broker/
│   ├── broker.go        # Core gRPC handlers: Produce, Consume, Join, offset management
│   ├── topic.go         # CreateTopic handler
│   ├── cg_rebalancer.go # Consumer group coordination: JoinGroup, SyncGroup, Heartbeat
│   └── constant.go      # Session/heartbeat/rebalance timeouts
├── cluster/
│   └── fsm.go           # Raft FSM — applies committed log entries to state
├── storage/
│   └── commitlog.go     # Append-only mmap'd log file per topic/partition
├── api/
│   ├── kafka.proto      # gRPC service and message definitions
│   ├── kafka.pb.go      # Generated protobuf code
│   └── kafka_grpc.pb.go # Generated gRPC stubs
├── client/
│   └── main.go          # CLI client: create-topic, produce, consume
├── data/                # Runtime data directory (Raft DB, log files)
└── go.mod
```

---

## Getting Started

### Prerequisites

- Go 1.24+
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (only needed to regenerate proto files)

### Running a 3-Node Cluster

Open three terminal windows.

**Terminal 1 — Start Node 1 (bootstraps the cluster):**
```bash
go run ./cmd \
  -id node1 \
  -grpc_addr 127.0.0.1:9092 \
  -raft_addr 127.0.0.1:19901 \
  -data_dir ./data
```

**Terminal 2 — Start Node 2 (joins via node1):**
```bash
go run ./cmd \
  -id node2 \
  -grpc_addr 127.0.0.1:9093 \
  -raft_addr 127.0.0.1:19902 \
  -join_addr 127.0.0.1:9092 \
  -data_dir ./data
```

**Terminal 3 — Start Node 3 (joins via node1 or node2):**
```bash
go run ./cmd \
  -id node3 \
  -grpc_addr 127.0.0.1:9094 \
  -raft_addr 127.0.0.1:19903 \
  -join_addr 127.0.0.1:9092 \
  -data_dir ./data
```

> Node 1 becomes the Raft leader because it bootstraps with `-join_addr` omitted. Nodes 2 and 3 contact node 1 (or any existing node) to join. If the contact node is not the leader, the join request is automatically redirected.

**Broker flags:**

| Flag | Default | Description |
|---|---|---|
| `-id` | (required) | Unique node ID within the cluster |
| `-grpc_addr` | `127.0.0.1:9092` | Address for client-facing gRPC server |
| `-raft_addr` | `127.0.0.1:19902` | Address for internal Raft TCP transport |
| `-join_addr` | `""` | gRPC address of any existing cluster node |
| `-data_dir` | `./data` | Root directory for log files and Raft state |

### Using the Client CLI

```bash
# Create a topic with 3 partitions
go run ./client create-topic \
  --bootstrap-server 127.0.0.1:9092 \
  --topic orders \
  --partitions 3

# Produce a message to partition 0
go run ./client produce \
  --bootstrap-server 127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094 \
  --topic orders \
  --acks all \
  0 "order-id-123"

# Consume from a group (run multiple instances to trigger rebalancing)
go run ./client consume \
  --bootstrap-server 127.0.0.1:9092 \
  --topic orders \
  --group my-consumer-group
```

**To see rebalancing in action**, open two terminals and run the `consume` command in both. Watch the logs — the broker will detect the new member, trigger a rebalance, and redistribute partitions across both consumers. Kill one consumer to watch the broker detect the timeout and rebalance again.

---

## gRPC API

Defined in `api/kafka.proto`:

```protobuf
service Kafka {
  // Client-facing
  rpc Produce(ProduceRequest)           returns (ProduceResponse);
  rpc Consume(ConsumeRequest)           returns (ConsumeResponse);
  rpc CommitOffset(CommitOffsetRequest) returns (CommitOffsetResponse);
  rpc FetchOffset(FetchOffsetRequest)   returns (FetchOffsetResponse);
  rpc CreateTopic(CreateTopicRequest)   returns (CreateTopicResponse);

  // Consumer group coordination
  rpc JoinGroup(JoinGroupRequest)       returns (JoinGroupResponse);
  rpc SyncGroup(SyncGroupRequest)       returns (SyncGroupResponse);
  rpc Heartbeat(HeartbeatRequest)       returns (HeartbeatResponse);

  // Internal cluster membership
  rpc Join(JoinRequest)                 returns (JoinResponse);
}
```

**Ack levels for producers:**

| Level | Behavior |
|---|---|
| `NONE` | Fire-and-forget — returns immediately without waiting for Raft commit |
| `ALL` | Waits until a majority of nodes have committed the message to their logs |

---

## Tech Stack

| Library | Purpose |
|---|---|
| [`hashicorp/raft`](https://github.com/hashicorp/raft) | Raft consensus algorithm for leader election and log replication |
| [`hashicorp/raft-boltdb`](https://github.com/hashicorp/raft-boltdb) | BoltDB-backed persistent store for Raft log |
| [`google.golang.org/grpc`](https://pkg.go.dev/google.golang.org/grpc) | gRPC framework for client-server and cluster communication |
| [`google.golang.org/protobuf`](https://pkg.go.dev/google.golang.org/protobuf) | Protocol Buffers serialization |
| [`golang.org/x/sys/unix`](https://pkg.go.dev/golang.org/x/sys/unix) | `mmap` / `munmap` / `msync` syscalls for zero-copy log I/O |
| [`google/uuid`](https://github.com/google/uuid) | Unique consumer IDs |
