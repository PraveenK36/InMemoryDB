package com.db.memory;

import com.db.memory.cluster.ClusterManager;
import com.db.memory.hashing.HashRing;
import com.db.memory.replication.ReplicationManager;
import com.db.memory.server.KVServer;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KVStoreIntegrationTest {

    @Container
    static GenericContainer<?> zookeeper = new GenericContainer<>(DockerImageName.parse("zookeeper:3.8"))
            .withExposedPorts(2181)
            .withEnv("ZOO_MY_ID", "1")
            .withEnv("ZOO_4LW_COMMANDS_WHITELIST", "*");

    private static String zkConnect;

    // block-1: leader on port 19001, replica on port 19002
    private static ClusterManager leaderCM;
    private static ClusterManager replicaCM;
    private static Map<String, String> leaderStore;
    private static Map<String, String> replicaStore;
    private static int leaderPort = 19001;
    private static int replicaPort = 19002;

    // block-2: leader on port 19003
    private static ClusterManager block2LeaderCM;
    private static Map<String, String> block2LeaderStore;
    private static int block2LeaderPort = 19003;

    @BeforeAll
    static void setUp() throws Exception {
        zkConnect = zookeeper.getHost() + ":" + zookeeper.getMappedPort(2181);

        // --- block-1 leader ---
        String leaderAddress = "localhost:" + leaderPort;
        leaderCM = new ClusterManager("block-1", leaderAddress, zkConnect);
        leaderCM.initialize(event -> {});

        boolean isLeader = leaderCM.tryToBecomeLeader();
        assertTrue(isLeader, "First node should become leader");
        leaderCM.registerNode("block-1", List.of("localhost:" + replicaPort), true);

        // --- block-1 replica ---
        String replicaAddress = "localhost:" + replicaPort;
        replicaCM = new ClusterManager("block-1", replicaAddress, zkConnect);
        replicaCM.initialize(event -> {});

        boolean replicaIsLeader = replicaCM.tryToBecomeLeader();
        assertFalse(replicaIsLeader, "Second node should NOT become leader");

        // --- block-2 leader ---
        String block2Address = "localhost:" + block2LeaderPort;
        block2LeaderCM = new ClusterManager("block-2", block2Address, zkConnect);
        block2LeaderCM.initialize(event -> {});

        boolean block2IsLeader = block2LeaderCM.tryToBecomeLeader();
        assertTrue(block2IsLeader, "First block-2 node should become leader");
        block2LeaderCM.registerNode("block-2", List.of(), true);

        // Build hash rings and start KVServers
        HashRing leaderRing = new HashRing(leaderCM);
        leaderRing.buildHashRing();
        HashRing replicaRing = new HashRing(replicaCM);
        replicaRing.buildHashRing();
        HashRing block2Ring = new HashRing(block2LeaderCM);
        block2Ring.buildHashRing();

        leaderStore = new HashMap<>();
        replicaStore = new HashMap<>();
        block2LeaderStore = new HashMap<>();

        ReplicationManager leaderRM = new ReplicationManager("block-1", leaderCM);
        ReplicationManager replicaRM = new ReplicationManager("block-1", replicaCM);
        ReplicationManager block2RM = new ReplicationManager("block-2", block2LeaderCM);

        KVServer leaderServer = new KVServer(leaderPort, leaderStore, leaderRM, leaderCM, leaderRing);
        KVServer replicaServer = new KVServer(replicaPort, replicaStore, replicaRM, replicaCM, replicaRing);
        KVServer block2Server = new KVServer(block2LeaderPort, block2LeaderStore, block2RM, block2LeaderCM, block2Ring);

        new Thread(leaderServer).start();
        new Thread(replicaServer).start();
        new Thread(block2Server).start();

        // Wait for servers to bind
        Thread.sleep(1000);
    }

    private String sendCommand(int port, String command) throws Exception {
        try (Socket socket = new Socket("localhost", port);
             PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            socket.setSoTimeout(5000);
            writer.println(command);
            return reader.readLine();
        }
    }

    /**
     * Find a key that the hash ring routes to a given block.
     */
    private String findKeyForBlock(String targetBlock, HashRing ring) {
        for (int i = 0; i < 1000; i++) {
            String candidate = "key-" + i;
            if (ring.getTargetNode(candidate).equals(targetBlock)) {
                return candidate;
            }
        }
        fail("Could not find a key that hashes to " + targetBlock);
        return null;
    }

    // =========================================================
    // Test 1: Leader accepts PUT
    // =========================================================
    @Test
    @Order(1)
    void leaderAcceptsPut() throws Exception {
        HashRing ring = new HashRing(leaderCM);
        ring.buildHashRing();
        String key = findKeyForBlock("block-1", ring);

        String response = sendCommand(leaderPort, "PUT " + key + " testvalue");
        assertEquals("OK", response);
    }

    // =========================================================
    // Test 2: Replica rejects PUT (leader fencing)
    // =========================================================
    @Test
    @Order(2)
    void replicaRejectsPut() throws Exception {
        HashRing ring = new HashRing(replicaCM);
        ring.buildHashRing();
        String key = findKeyForBlock("block-1", ring);

        String response = sendCommand(replicaPort, "PUT " + key + " testvalue");
        assertTrue(response.startsWith("ERROR"), "Replica should reject PUT, got: " + response);
    }

    // =========================================================
    // Test 3: GET returns correct value from leader
    // =========================================================
    @Test
    @Order(3)
    void getFromLeaderReturnsValue() throws Exception {
        HashRing ring = new HashRing(leaderCM);
        ring.buildHashRing();
        String key = findKeyForBlock("block-1", ring);

        sendCommand(leaderPort, "PUT " + key + " apple");
        String response = sendCommand(leaderPort, "GET " + key);
        assertEquals("apple", response);
    }

    // =========================================================
    // Test 4: Replication - data available on replica after PUT on leader
    // =========================================================
    @Test
    @Order(4)
    void replicationWorksFromLeaderToReplica() throws Exception {
        HashRing ring = new HashRing(leaderCM);
        ring.buildHashRing();
        String key = findKeyForBlock("block-1", ring);

        sendCommand(leaderPort, "PUT " + key + " replicated-value");

        // Give replication a moment
        Thread.sleep(500);

        String response = sendCommand(replicaPort, "GET " + key);
        assertEquals("replicated-value", response);
    }

    // =========================================================
    // Test 5: DELETE works on leader
    // =========================================================
    @Test
    @Order(5)
    void deleteOnLeader() throws Exception {
        HashRing ring = new HashRing(leaderCM);
        ring.buildHashRing();
        String key = findKeyForBlock("block-1", ring);

        sendCommand(leaderPort, "PUT " + key + " to-delete");
        String before = sendCommand(leaderPort, "GET " + key);
        assertEquals("to-delete", before);

        String deleteResponse = sendCommand(leaderPort, "DELETE " + key);
        assertEquals("OK", deleteResponse);

        String after = sendCommand(leaderPort, "GET " + key);
        assertEquals("NULL", after);
    }

    // =========================================================
    // Test 6: DELETE replicates to replica
    // =========================================================
    @Test
    @Order(6)
    void deleteReplicatesToReplica() throws Exception {
        HashRing ring = new HashRing(leaderCM);
        ring.buildHashRing();
        String key = findKeyForBlock("block-1", ring);

        sendCommand(leaderPort, "PUT " + key + " delete-me");
        Thread.sleep(500);
        assertEquals("delete-me", sendCommand(replicaPort, "GET " + key));

        sendCommand(leaderPort, "DELETE " + key);
        Thread.sleep(500);
        assertEquals("NULL", sendCommand(replicaPort, "GET " + key));
    }

    // =========================================================
    // Test 7: Replica rejects DELETE (leader fencing)
    // =========================================================
    @Test
    @Order(7)
    void replicaRejectsDelete() throws Exception {
        HashRing ring = new HashRing(replicaCM);
        ring.buildHashRing();
        String key = findKeyForBlock("block-1", ring);

        String response = sendCommand(replicaPort, "DELETE " + key);
        assertTrue(response.startsWith("ERROR"), "Replica should reject DELETE");
    }

    // =========================================================
    // Test 8: GET returns NULL for nonexistent key
    // =========================================================
    @Test
    @Order(8)
    void getNonexistentKeyReturnsNull() throws Exception {
        String response = sendCommand(leaderPort, "GET nonexistent-key-xyz");
        assertEquals("NULL", response);
    }

    // =========================================================
    // Test 9: Block-2 leader accepts PUT for block-2 keys
    // =========================================================
    @Test
    @Order(9)
    void block2LeaderAcceptsPut() throws Exception {
        HashRing ring = new HashRing(block2LeaderCM);
        ring.buildHashRing();
        String key = findKeyForBlock("block-2", ring);

        String response = sendCommand(block2LeaderPort, "PUT " + key + " block2value");
        assertEquals("OK", response);

        String getResponse = sendCommand(block2LeaderPort, "GET " + key);
        assertEquals("block2value", getResponse);
    }

    // =========================================================
    // Test 10: Block-2 leader rejects PUT for block-1 keys
    // =========================================================
    @Test
    @Order(10)
    void block2LeaderRejectsBlock1Keys() throws Exception {
        HashRing ring = new HashRing(block2LeaderCM);
        ring.buildHashRing();
        String key = findKeyForBlock("block-1", ring);

        String response = sendCommand(block2LeaderPort, "PUT " + key + " wrong-block");
        assertTrue(response.startsWith("ERROR"), "Block-2 leader should reject keys owned by block-1");
    }

    // =========================================================
    // Test 11: Unknown command returns error
    // =========================================================
    @Test
    @Order(11)
    void unknownCommandReturnsError() throws Exception {
        String response = sendCommand(leaderPort, "INVALID foo bar");
        assertEquals("ERROR: Unknown command", response);
    }

    // =========================================================
    // Test 12: Leader election stores unique address, not block name
    // =========================================================
    @Test
    @Order(12)
    void leaderElectionStoresUniqueAddress() throws Exception {
        String leaderData = leaderCM.getCurrentLeader("block-1");
        assertNotNull(leaderData);
        assertEquals("localhost:" + leaderPort, leaderData,
                "Leader ZNode should store the unique address, not the block name");
        assertNotEquals("block-1", leaderData,
                "Leader ZNode must NOT store the shared block name");
    }

    // =========================================================
    // Test 13: Multiple PUTs overwrite value
    // =========================================================
    @Test
    @Order(13)
    void putOverwritesPreviousValue() throws Exception {
        HashRing ring = new HashRing(leaderCM);
        ring.buildHashRing();
        String key = findKeyForBlock("block-1", ring);

        sendCommand(leaderPort, "PUT " + key + " first");
        assertEquals("first", sendCommand(leaderPort, "GET " + key));

        sendCommand(leaderPort, "PUT " + key + " second");
        assertEquals("second", sendCommand(leaderPort, "GET " + key));
    }

    // =========================================================
    // Test 14: Failover - replica becomes leader when leader ZNode is deleted
    // =========================================================
    @Test
    @Order(14)
    void failoverReplicaBecomesLeader() throws Exception {
        // Set up a separate block for this test to avoid interfering with others
        String blockId = "block-failover";

        String originalLeaderAddr = "localhost:19010";
        ClusterManager originalLeaderCM = new ClusterManager(blockId, originalLeaderAddr, zkConnect);
        originalLeaderCM.initialize(event -> {});
        assertTrue(originalLeaderCM.tryToBecomeLeader());
        originalLeaderCM.registerNode(blockId, List.of(), true);

        String replicaAddr = "localhost:19011";
        ClusterManager failoverReplicaCM = new ClusterManager(blockId, replicaAddr, zkConnect);
        failoverReplicaCM.initialize(event -> {});
        assertFalse(failoverReplicaCM.tryToBecomeLeader());

        // Verify original leader is in charge
        assertEquals(originalLeaderAddr, failoverReplicaCM.getCurrentLeader(blockId));

        // Set up watch so replica tries to take over
        failoverReplicaCM.watchLeadership(blockId, () -> {
            try {
                failoverReplicaCM.tryToBecomeLeader();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Simulate leader failure by closing its ZooKeeper session
        originalLeaderCM.getZooKeeper().close();

        // Wait for ZK to detect session loss and replica to take over
        Thread.sleep(5000);

        String newLeader = failoverReplicaCM.getCurrentLeader(blockId);
        assertEquals(replicaAddr, newLeader, "Replica should have become the new leader after failover");
    }
}
