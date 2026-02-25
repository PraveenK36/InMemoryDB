# InMemory Distributed Key-Value Store

A prototype in-memory distributed key-value store with consistent hashing, replication, and leader election using ZooKeeper — fully containerized with Docker Compose.

---

## Features
- Consistent hashing with virtual nodes (vNodes)
- Node registration and discovery using Apache ZooKeeper
- Leader election per partition (block) with failover
- Leader fencing — only the elected leader accepts writes for its partition
- TCP-based replication from leader to replicas
- Dockerized multi-node cluster

---

## Architecture

The cluster is organized into **partitions** (called blocks). Each block has one leader and one or more replicas:

| Container | Partition | Port | Replica(s) |
|---|---|---|---|
| kvstore-node-1 | block-1 | 9001 | kvstore-node-2:9002 |
| kvstore-node-2 | block-1 | 9002 | kvstore-node-1:9001 |
| kvstore-node-3 | block-2 | 9003 | kvstore-node-4:9004 |
| kvstore-node-4 | block-2 | 9004 | kvstore-node-3:9003 |
| kvstore-node-5 | block-1 | 9005 | kvstore-node-3:9001,kvstore-node-3:9002 |

Within each block, the first node to start wins the leader election. Replicas watch the leader ZNode and attempt to take over if the leader fails.

---

## Requirements
- Docker & Docker Compose

---

## Setup

### 1. Build and start the cluster
```bash
docker compose up --build
```

This starts ZooKeeper and all 5 KV store nodes. ZooKeeper must be healthy before nodes start (handled via healthcheck).

### 2. Stop the cluster
```bash
docker compose down
```

To also remove persisted ZooKeeper data:
```bash
docker compose down -v
```

---

## Usage

### PUT a key
```bash
echo "PUT apple red" | nc localhost 9001
```

### GET a key
```bash
echo "GET apple" | nc localhost 9002
```

### DELETE a key
```bash
echo "DELETE apple" | nc localhost 9001
```

**Note:** PUT and DELETE are only accepted by the current leader for the key's partition. Non-leaders return an error. GET works on any node that holds the data (leader or replica).

---

## Testing Failover

1. Write a key to the leader:
   ```bash
   echo "PUT apple red" | nc localhost 9001
   ```

2. Kill the leader:
   ```bash
   docker stop kvstore-node-1
   ```

3. The replica detects the leader is gone and wins the election. Write to the new leader:
   ```bash
   echo "PUT banana yellow" | nc localhost 9002
   ```

---

## How It Works

- **Consistent Hashing**: Keys are mapped to partitions via a SHA-256 hash ring with 10 virtual nodes per partition.
- **Leader Election**: Each node attempts to create an ephemeral ZNode at `/leaders/<partition>`. The first to succeed becomes leader, storing its unique `hostname:port` address.
- **Leader Fencing**: On writes, the server checks if its own address matches the address stored in the leader ZNode. Only the actual leader can accept writes.
- **Replication**: The leader forwards writes to replicas using a `REPLICATE` prefix so replicas can distinguish replication traffic from client requests.
- **Failover**: Replicas watch the leader ZNode. When it's deleted (leader crashed), they race to create a new one.

---

## Project Structure
```
src/main/java/com/db/memory/
  KVNode.java              # Entry point, node startup and coordination
  cluster/
    ClusterManager.java    # ZooKeeper connection, leader election, node registration
  hashing/
    HashRing.java          # Consistent hash ring for key-to-partition mapping
  replication/
    ReplicationManager.java # TCP-based replication to replica nodes
  server/
    KVServer.java          # TCP server handling PUT/GET/DELETE commands
```

---

## TODO
- Rebuild hash ring on topology changes (new partitions added at runtime)
- Dynamic replica discovery
- Graceful node shutdown and handover
- CLI tool for sending commands
