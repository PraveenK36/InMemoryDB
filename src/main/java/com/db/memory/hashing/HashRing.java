package com.db.memory.hashing;

import com.db.memory.cluster.ClusterManager;
import org.apache.zookeeper.KeeperException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

public class HashRing {
    private final SortedMap<Integer, String> ring = new TreeMap<>();
    private static final int NUM_VIRTUAL_NODES = 10;
    private final ClusterManager clusterManager;

    public HashRing(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void buildHashRing() throws KeeperException, InterruptedException {
        ring.clear();
        List<String> leaderNodeIds = clusterManager.getZooKeeper().getChildren("/leaders", false);
        for (String nodeId : leaderNodeIds) {
            for (int i = 0; i < NUM_VIRTUAL_NODES; i++) {
                String vnode = nodeId + "-vnode-" + i;
                int hash = hash(vnode);
                ring.put(hash, nodeId);
            }
        }
        System.out.println("[HashRing] Built ring with nodes: " + ring);
    }

    public int hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            return Math.abs(ByteBuffer.wrap(digest).getInt());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getTargetNode(String key) {
        int hash = hash(key);
        SortedMap<Integer, String> tailMap = ring.tailMap(hash);
        return tailMap.isEmpty() ? ring.get(ring.firstKey()) : tailMap.get(tailMap.firstKey());
    }

    public List<Integer> getUniqueShardIds() {
        Set<Integer> shardIds = new HashSet<>();
        for (int hash : ring.keySet()) {
            shardIds.add(hash % 10);
        }
        return new ArrayList<>(shardIds);
    }
}
