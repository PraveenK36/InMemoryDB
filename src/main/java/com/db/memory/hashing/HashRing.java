package com.db.memory.hashing;

import com.db.memory.cluster.ClusterManager;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

public class HashRing {
    private final ClusterManager clusterManager;
    private final SortedMap<Integer, String> ring = new TreeMap<>();
    private static final int NUM_VIRTUAL_NODES = 10;

    public HashRing(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void build() throws Exception {
        ring.clear();
        List<String> nodes = clusterManager.getAllNodes();
        for (String node : nodes) {
            for (int i = 0; i < NUM_VIRTUAL_NODES; i++) {
                String vnode = node + "-vnode-" + i;
                int hash = hash(vnode);
                ring.put(hash, node);
            }
        }
        System.out.println("[" + clusterManager.getNodeId() + "] Built hash ring: " + ring);
    }

    public String getTargetNode(String key) {
        int hash = hash(key);
        SortedMap<Integer, String> tailMap = ring.tailMap(hash);
        return tailMap.isEmpty() ? ring.get(ring.firstKey()) : tailMap.get(tailMap.firstKey());
    }

    public Set<Integer> getUniqueShards() {
        Set<Integer> shards = new HashSet<>();
        for (Integer hash : ring.keySet()) {
            shards.add(hash % 10); // define shard granularity here
        }
        return shards;
    }

    private int hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            return Math.abs(ByteBuffer.wrap(digest).getInt());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<Integer> getUniqueShardIds() {
        Set<Integer> shardIds = new HashSet<>();
        for (int hash : ring.keySet()) {
            shardIds.add(hash % 10); // or configurable SHARD_COUNT
        }
        return new ArrayList<>(shardIds);
    }

}
