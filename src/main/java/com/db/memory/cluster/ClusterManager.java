package com.db.memory.cluster;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * ClusterManager handles node registration and metadata storage in ZooKeeper.
 * Each node creates an ephemeral node containing metadata (e.g. port, replica info).
 * Also supports node-level leadership election and failover.
 */
public class ClusterManager {
    private final String nodeId;
    private final String nodeAddress;
    private final ZooKeeper zooKeeper;
    private static final String ZK_NODES_PATH = "/nodes";
    private static final String ZK_LEADERS_PATH = "/leaders";

    public ClusterManager(String nodeId, String nodeAddress, String zkConnect) throws Exception {
        this.nodeId = nodeId;
        this.nodeAddress = nodeAddress;
        CountDownLatch connectedLatch = new CountDownLatch(1);
        this.zooKeeper = new ZooKeeper(zkConnect, 30000, event -> {
            System.out.println("[" + nodeId + "] ZooKeeper event: " + event);
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedLatch.countDown();
            }
        });

        // Wait for connection (max 30 seconds)
        if (!connectedLatch.await(30, TimeUnit.SECONDS)) {
            throw new Exception("Could not connect to ZooKeeper within 30 seconds");
        }

        System.out.println("[" + nodeId + "] Connected to ZooKeeper");
    }

    public void initialize(Watcher watcher) throws KeeperException, InterruptedException {
        createIfNotExists(ZK_NODES_PATH);
        createIfNotExists(ZK_LEADERS_PATH);
        zooKeeper.register(watcher);
    }

    public void registerNode(String nodeId, List<String> replicas, boolean isLeader) throws KeeperException, InterruptedException {
        if (!isLeader) {
            System.out.println("[" + nodeId + "] Skipping registration — not the leader.");
            return;
        }

        String nodePath = ZK_NODES_PATH + "/" + nodeId;
        StringBuilder sb = new StringBuilder();
        sb.append(nodeId); // leader node
        if (replicas != null && !replicas.isEmpty()) {
            sb.append("|" + String.join(",", replicas));
        }
        byte[] metadata = sb.toString().getBytes(StandardCharsets.UTF_8);

        if (zooKeeper.exists(nodePath, false) == null) {
            zooKeeper.create(nodePath, metadata, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
            zooKeeper.setData(nodePath, metadata, -1);
        }

        System.out.println("[" + nodeId + "] Registered with ZooKeeper at path: " + nodePath + " with data: " + sb);
    }


    public boolean tryToBecomeLeader() throws KeeperException, InterruptedException {
        String path = ZK_LEADERS_PATH + "/" + nodeId;
        try {
            zooKeeper.create(path, nodeAddress.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("[" + nodeId + "] Became leader.");
            return true;
        } catch (KeeperException.NodeExistsException e) {
            return false;
        }
    }

    public void watchLeadership(String targetNodeId, Runnable onLeaderGone) throws KeeperException, InterruptedException {
        String path = ZK_LEADERS_PATH + "/" + targetNodeId;
        Watcher watcher = event -> {
            if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                System.out.println("[" + nodeId + "] Detected leader node deleted for node " + targetNodeId);
                onLeaderGone.run();
                try {
                    // Re-set the watch
                    watchLeadership(targetNodeId, onLeaderGone);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        Stat stat = zooKeeper.exists(path, watcher);
        if (stat == null) {
            // Leader already gone, invoke immediately
            onLeaderGone.run();
        }
    }

    public String getCurrentLeader(String targetNodeId) throws KeeperException, InterruptedException {
        String path = ZK_LEADERS_PATH + "/" + targetNodeId;
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) return null;
        byte[] data = zooKeeper.getData(path, false, null);
        return new String(data, StandardCharsets.UTF_8);
    }

    private void createIfNotExists(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    public Map<String, NodeInfo> getNodeMetadata() throws KeeperException, InterruptedException {
        Map<String, NodeInfo> nodeMap = new HashMap<>();
        List<String> nodes = zooKeeper.getChildren(ZK_NODES_PATH, false);
        for (String node : nodes) {
            String path = ZK_NODES_PATH + "/" + node;
            byte[] data = zooKeeper.getData(path, false, null);
            String[] parts = new String(data, StandardCharsets.UTF_8).split("\\|");
            List<String> replicas = new ArrayList<>();
            if (parts.length > 1) {
                Collections.addAll(replicas, parts[1].split(","));
            }
            nodeMap.put(node, new NodeInfo(node, replicas));
        }
        return nodeMap;
    }

    public List<String> getAllNodes() throws KeeperException, InterruptedException {
        return zooKeeper.getChildren(ZK_NODES_PATH, false);
    }

    public ZooKeeper getZooKeeper() {
        return this.zooKeeper;
    }

    public String getNodeId() {
        return this.nodeId;
    }

    public String getNodeAddress() {
        return this.nodeAddress;
    }

    public static class NodeInfo {
        public final String nodeId;
        public final List<String> replicas;

        public NodeInfo(String nodeId, List<String> replicas) {
            this.nodeId = nodeId;
            this.replicas = replicas;
        }

        @Override
        public String toString() {
            return "NodeInfo{" +
                    "nodeId='" + nodeId + '\'' +
                    ", replicas=" + replicas +
                    '}';
        }
    }
}
