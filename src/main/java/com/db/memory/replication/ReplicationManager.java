package com.db.memory.replication;

import com.db.memory.cluster.ClusterManager;
import com.db.memory.cluster.ClusterManager.NodeInfo;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

public class ReplicationManager {
    private final String nodeId;
    private final ClusterManager clusterManager;
    private static final int MAX_RETRIES = 3;
    private static final int BACKOFF_MS = 500;

    public ReplicationManager(String nodeId, ClusterManager clusterManager) {
        this.nodeId = nodeId;
        this.clusterManager = clusterManager;
    }

    public void replicatePut(String key, String value) {
        try {
            NodeInfo self = clusterManager.getNodeMetadata().get(nodeId);
            for (String replica : self.replicas) {
                sendWithRetry(replica, "PUT " + key + " " + value);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("[" + nodeId + "] Failed to replicate PUT: " + e.getMessage());
        }
    }

    public void replicateDelete(String key) {
        try {
            NodeInfo self = clusterManager.getNodeMetadata().get(nodeId);
            for (String replica : self.replicas) {
                sendWithRetry(replica, "DELETE " + key);
            }
        } catch (Exception e) {
            System.err.println("[" + nodeId + "] Failed to replicate DELETE: " + e.getMessage());
        }
    }

    private void sendWithRetry(String replica, String command) {
        int attempt = 0;
        String[] split = replica.split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]);
        while (attempt < MAX_RETRIES) {
            try (Socket socket = new Socket(host, port);
                 PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true)) {
                writer.println(command);
                return;
            } catch (Exception e) {
                attempt++;
                System.err.println("[" + nodeId + "] Retry " + attempt + " failed to host " + host + " and port " + port + ": " + e.getMessage());
                try {
                    Thread.sleep(BACKOFF_MS);
                } catch (InterruptedException ignored) {
                }
            }
        }
        System.err.println("[" + nodeId + "] Final failure sending to port " + port + " after retries.");
    }
}
