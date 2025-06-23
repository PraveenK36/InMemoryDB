package com.db.memory.replication;

import com.db.memory.cluster.ClusterManager;
import org.apache.zookeeper.ZooKeeper;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;

public class ReplicationManager {
    private final String nodeId;
    private final ClusterManager clusterManager;
    private final ZooKeeper zooKeeper;
    private static final String ZK_NODES_PATH = "/nodes";

    public ReplicationManager(String nodeId, ClusterManager clusterManager) {
        this.nodeId = nodeId;
        this.clusterManager = clusterManager;
        this.zooKeeper = clusterManager.getZooKeeper();
    }

    public void replicatePut(String key, String value) {
        broadcast("REPLICATE_PUT " + key + " " + value);
    }

    public void replicateDelete(String key) {
        broadcast("REPLICATE_DELETE " + key);
    }

    private void broadcast(String command) {
        try {
            List<String> nodes = zooKeeper.getChildren(ZK_NODES_PATH, false);
            for (String peer : nodes) {
                if (!peer.equals(nodeId)) {
                    String data = new String(zooKeeper.getData(ZK_NODES_PATH + "/" + peer, false, null));
                    int port = Integer.parseInt(data);
                    sendCommand(peer, port, command);
                }
            }
        } catch (Exception e) {
            System.err.println("[" + nodeId + "] Failed to fetch peer nodes from ZooKeeper: " + e.getMessage());
        }
    }

    private void sendCommand(String replica, int port, String command) {
        try (Socket socket = new Socket("localhost", port);
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {
            writer.println(command);
            System.out.println("[" + nodeId + "] Replicated to " + replica + ": " + command);
        } catch (Exception e) {
            System.err.println("[" + nodeId + "] Failed to replicate to " + replica + " on port " + port);
        }
    }
}
