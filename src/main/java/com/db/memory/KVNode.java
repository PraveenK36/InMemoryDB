package com.db.memory;

import com.db.memory.cluster.ClusterManager;
import com.db.memory.hashing.HashRing;
import com.db.memory.replication.ReplicationManager;
import com.db.memory.server.KVServer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Refactored KVNode: Entry point for a distributed key-value store node
 */
public class KVNode implements Watcher {

    private final String nodeId;
    private final Map<String, String> store;
    private final ClusterManager clusterManager;
    private final HashRing hashRing;
    private final ReplicationManager replicationManager;
    private final KVServer kvServer;
    private final int port;

    public KVNode(String nodeId, String zkConnectString, int port) throws Exception {
        this.nodeId = nodeId;
        this.port = port;
        this.store = new ConcurrentHashMap<>();
        this.clusterManager = new ClusterManager(nodeId, zkConnectString);
        this.hashRing = new HashRing(clusterManager);
        this.replicationManager = new ReplicationManager(nodeId, clusterManager);
        this.kvServer = new KVServer(port, store, replicationManager);

        clusterManager.initialize(this);
        clusterManager.registerNode(port);
        hashRing.build();
        clusterManager.electLeadership(hashRing.getUniqueShardIds());
        rebalanceKeys();
        new Thread(kvServer).start();
    }

    private void rebalanceKeys() {
        store.keySet().removeIf(key -> {
            String rightfulOwner = hashRing.getTargetNode(key);
            if (!rightfulOwner.equals(nodeId)) {
                System.out.println("[" + nodeId + "] Rebalancing key='" + key + "' to node='" + rightfulOwner + "'");
                return true;
            }
            return false;
        });
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("[" + nodeId + "] ZooKeeper event: " + event);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java KVNode <nodeId> <zookeeper-host:port> <tcp-port>");
            System.exit(1);
        }

        String nodeId = args[0];
        String zkConnect = args[1];
        int port = Integer.parseInt(args[2]);

        new KVNode(nodeId, zkConnect, port);
        Thread.sleep(Long.MAX_VALUE); // keep running
    }
}
