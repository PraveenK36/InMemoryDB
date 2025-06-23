package com.db.memory.cluster;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class ClusterManager {
    private final String nodeId;
    private final ZooKeeper zooKeeper;
    private static final String ZK_NODES_PATH = "/nodes";
    private static final String ZK_LEADER_PATH = "/leaders";

    public ClusterManager(String nodeId, String zkConnect) throws Exception {
        this.nodeId = nodeId;
        this.zooKeeper = new ZooKeeper(zkConnect, 3000, event -> {
            System.out.println("[" + nodeId + "] ZooKeeper event: " + event);
        });
    }

    public void initialize(Watcher watcher) throws KeeperException, InterruptedException {
        createIfNotExists(ZK_NODES_PATH);
        createIfNotExists(ZK_LEADER_PATH);
        zooKeeper.register(watcher);
    }

    public void registerNode(int port) throws KeeperException, InterruptedException {
        String nodePath = ZK_NODES_PATH + "/" + nodeId;
        byte[] portData = String.valueOf(port).getBytes();
        if (zooKeeper.exists(nodePath, false) == null) {
            zooKeeper.create(nodePath, portData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
            zooKeeper.setData(nodePath, portData, -1);
        }
        System.out.println("[" + nodeId + "] Registered with ZooKeeper at path: " + nodePath + " with port: " + port);
    }

    private void createIfNotExists(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    public void electLeadership(List<Integer> shardIds) {
        for (int shardId : shardIds) {
            String leaderPath = ZK_LEADER_PATH + "/shard-" + shardId;
            try {
                zooKeeper.create(leaderPath, nodeId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                System.out.println("[" + nodeId + "] Became leader for shard-" + shardId);
            } catch (KeeperException.NodeExistsException ignored) {
                // another node is already the leader
            } catch (Exception e) {
                System.err.println("[" + nodeId + "] Leadership election error: " + e.getMessage());
            }
        }
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
}
