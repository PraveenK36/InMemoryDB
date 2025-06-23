# InMemory Distributed Key-Value Store

This is a simple prototype of an in-memory distributed key-value store with sharding, replication, and leader election using ZooKeeper.

---

## ✅ Features Implemented
- Consistent hashing with virtual nodes (vNodes)
- Node registration and discovery using Apache ZooKeeper
- Leader election per shard
- TCP server (`KVServer`) to handle all PUT, GET, DELETE commands
- Data replication between nodes

---

## 🧪 Requirements
- Java 11+
- Maven
- ZooKeeper running locally or remotely

---

## 🚀 Setup Instructions

### 1. Build the fat JAR
```bash
mvn clean package
```
The output JAR will be at:
```
target/InMemoryDB-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### 2. Start ZooKeeper (if not already running)
```bash
./scripts/setup-zookeeper.sh
```

### 3. Start nodes
Start each node in a separate terminal:
```bash
java -cp target/InMemoryDB-1.0-SNAPSHOT-jar-with-dependencies.jar com.db.memory.KVNode node-1 localhost:2181 9001
java -cp target/InMemoryDB-1.0-SNAPSHOT-jar-with-dependencies.jar com.db.memory.KVNode node-2 localhost:2181 9002
```

---

## 📥 Send Requests via Netcat

### PUT a key:
```bash
echo "PUT apple red" | nc localhost 9001
```

### GET from another node:
```bash
echo "GET apple" | nc localhost 9002
```

### DELETE a key:
```bash
echo "DELETE apple" | nc localhost 9001
```

---

## 📄 Notes
- Nodes write their TCP ports as ephemeral data to ZooKeeper at `/nodes/<nodeId>`
- Replication is done over TCP on localhost
- Each node accepts both client and peer connections on the same port

---

## 📌 TODO
- Add dynamic re-sharding with key migration
- Graceful node shutdown and handover
- CLI tool for sending commands easily
