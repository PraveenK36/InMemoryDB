package com.db.memory;

import com.db.memory.cluster.ClusterManager;
import com.db.memory.hashing.HashRing;
import com.db.memory.replication.ReplicationManager;
import com.db.memory.server.KVServer;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KVNode {
    private final String nodeId;
    private final String zkConnect;
    private final int port;
    private final List<String> replicas;
    private final Map<String, String> store = new HashMap<>();

    public KVNode(String nodeId, String zkConnect, int port, List<String> replicas) {
        this.nodeId = nodeId;
        this.zkConnect = zkConnect;
        this.port = port;
        this.replicas = replicas;
    }

    public void start() throws Exception {
        String hostname = InetAddress.getLocalHost().getHostName();
        String nodeAddress = hostname + ":" + port;
        ClusterManager clusterManager = new ClusterManager(nodeId, nodeAddress, zkConnect);
        clusterManager.initialize(event -> {
        });

        // Try to become leader for this physical node
        boolean isLeader = clusterManager.tryToBecomeLeader();
        if (isLeader) {
            clusterManager.registerNode(nodeId, replicas, true);
        }

        // Build hash ring after leader election so leaders are visible
        HashRing hashRing = new HashRing(clusterManager);
        hashRing.buildHashRing();
        clusterManager.watchLeadership(nodeId, () -> {
            try {
                boolean won = clusterManager.tryToBecomeLeader();
                if (won) {
                    clusterManager.registerNode(nodeId, replicas, true);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


        ReplicationManager replicationManager = new ReplicationManager(nodeId, clusterManager);
        KVServer kvServer = new KVServer(port, store, replicationManager, clusterManager, hashRing);
        new Thread(kvServer).start();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Usage: java KVNode <nodeId> <zkConnect> <nodeId> <replica,replica,...>");
            return;
        }

        String nodeId = args[0];
        String zkConnect = args[1];
        int port = Integer.parseInt(args[2]);
        List<String> replicas = Arrays.asList(args[3].split(","));
        KVNode node = new KVNode(nodeId, zkConnect, port, replicas);
        node.start();
    }
}
