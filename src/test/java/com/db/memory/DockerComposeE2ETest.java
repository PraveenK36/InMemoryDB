package com.db.memory;

import org.junit.jupiter.api.*;

import java.io.*;
import java.net.Socket;
import java.time.Duration;
import java.util.*;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * True end-to-end integration test that boots the full docker-compose stack
 * (ZooKeeper + 5 KV nodes) and tests against the running containers via TCP.
 *
 * Uses the actual docker-compose.yml via the docker compose CLI.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DockerComposeE2ETest {

    static final int NODE1_PORT = 9001;
    static final int NODE2_PORT = 9002;
    static final int NODE3_PORT = 9003;
    static final int NODE4_PORT = 9004;
    static final int NODE5_PORT = 9005;

    static final int[] BLOCK1_PORTS = {NODE1_PORT, NODE2_PORT, NODE5_PORT};
    static final int[] BLOCK2_PORTS = {NODE3_PORT, NODE4_PORT};

    static int block1LeaderPort;
    static int block2LeaderPort;
    static String block1Key;
    static String block2Key;

    @BeforeAll
    static void setUp() throws Exception {
        // Tear down any leftover containers, then build and start fresh
        runCompose("down", "-v");
        runCompose("up", "--build", "-d");

        // Wait for all nodes to be reachable
        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofSeconds(3))
                .ignoreExceptions()
                .untilAsserted(() -> {
                    for (int port : new int[]{NODE1_PORT, NODE2_PORT, NODE3_PORT, NODE4_PORT, NODE5_PORT}) {
                        assertNotNull(sendCommand(port, "GET ping"));
                    }
                });

        // Extra time for leader election and hash ring build
        Thread.sleep(3000);

        // Discover leaders first
        block1LeaderPort = discoverLeader(BLOCK1_PORTS, "probe-block1");
        block2LeaderPort = discoverLeader(BLOCK2_PORTS, "probe-block2");

        // Then find keys that exclusively belong to each block
        block1Key = findKeyForBlock(block1LeaderPort);
        block2Key = findKeyForBlock(block2LeaderPort);

        System.out.println("Block-1 leader: " + block1LeaderPort + ", key: " + block1Key);
        System.out.println("Block-2 leader: " + block2LeaderPort + ", key: " + block2Key);
    }

    @AfterAll
    static void tearDown() throws Exception {
        runCompose("down", "-v");
    }

    // =========================================================
    // Helpers
    // =========================================================

    static void runCompose(String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add("docker");
        cmd.add("compose");
        cmd.addAll(Arrays.asList(args));

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.directory(new File(System.getProperty("user.dir")));
        pb.inheritIO();
        Process p = pb.start();
        int exit = p.waitFor();
        if (exit != 0 && !args[0].equals("down")) {
            fail("docker compose " + String.join(" ", args) + " failed with exit code " + exit);
        }
    }

    static String sendCommand(int port, String command) throws Exception {
        try (Socket socket = new Socket("localhost", port);
             PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            socket.setSoTimeout(5000);
            writer.println(command);
            return reader.readLine();
        }
    }

    static int discoverLeader(int[] ports, String probePrefix) throws Exception {
        for (int i = 0; i < 100; i++) {
            String probeKey = probePrefix + "-" + i;
            for (int port : ports) {
                try {
                    String resp = sendCommand(port, "PUT " + probeKey + " probe");
                    if ("OK".equals(resp)) {
                        sendCommand(port, "DELETE " + probeKey);
                        return port;
                    }
                } catch (Exception ignored) {
                }
            }
        }
        fail("Could not find a leader among ports " + Arrays.toString(ports));
        return -1;
    }

    /**
     * Find a key that the given leader accepts AND the other block's leader rejects.
     * This ensures the key truly belongs to only one block.
     */
    static String findKeyForBlock(int leaderPort) throws Exception {
        int otherLeaderPort = (leaderPort == block1LeaderPort) ? block2LeaderPort : block1LeaderPort;
        for (int i = 0; i < 1000; i++) {
            String candidate = "testkey-" + i;
            try {
                String resp = sendCommand(leaderPort, "PUT " + candidate + " probe");
                if ("OK".equals(resp)) {
                    sendCommand(leaderPort, "DELETE " + candidate);
                    // Verify the other block rejects this key
                    String otherResp = sendCommand(otherLeaderPort, "PUT " + candidate + " probe");
                    if (otherResp != null && otherResp.startsWith("ERROR")) {
                        return candidate;
                    } else if ("OK".equals(otherResp)) {
                        sendCommand(otherLeaderPort, "DELETE " + candidate);
                    }
                }
            } catch (Exception ignored) {
            }
        }
        fail("Could not find a key exclusively accepted by port " + leaderPort);
        return null;
    }

    static int getBlock1ReplicaPort() {
        for (int port : BLOCK1_PORTS) {
            if (port != block1LeaderPort) return port;
        }
        fail("No replica found");
        return -1;
    }

    // =========================================================
    // Test 1: All nodes are reachable
    // =========================================================
    @Test
    @Order(1)
    void allNodesReachable() {
        for (int port : new int[]{NODE1_PORT, NODE2_PORT, NODE3_PORT, NODE4_PORT, NODE5_PORT}) {
            assertDoesNotThrow(() -> sendCommand(port, "GET ping"),
                    "Node on port " + port + " should be reachable");
        }
    }

    // =========================================================
    // Test 2: Leader accepts PUT
    // =========================================================
    @Test
    @Order(2)
    void leaderAcceptsPut() throws Exception {
        assertEquals("OK", sendCommand(block1LeaderPort, "PUT " + block1Key + " red"));
    }

    // =========================================================
    // Test 3: Replica rejects PUT (leader fencing)
    // =========================================================
    @Test
    @Order(3)
    void replicaRejectsPut() throws Exception {
        int replicaPort = getBlock1ReplicaPort();
        String resp = sendCommand(replicaPort, "PUT " + block1Key + " red");
        assertTrue(resp.startsWith("ERROR"), "Replica should reject PUT, got: " + resp);
    }

    // =========================================================
    // Test 4: GET from leader returns value
    // =========================================================
    @Test
    @Order(4)
    void getFromLeader() throws Exception {
        sendCommand(block1LeaderPort, "PUT " + block1Key + " apple");
        assertEquals("apple", sendCommand(block1LeaderPort, "GET " + block1Key));
    }

    // =========================================================
    // Test 5: Replication - GET from replica returns value
    // =========================================================
    @Test
    @Order(5)
    void replicationToReplica() throws Exception {
        sendCommand(block1LeaderPort, "PUT " + block1Key + " replicated");
        int replicaPort = getBlock1ReplicaPort();

        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(300))
                .ignoreExceptions()
                .untilAsserted(() ->
                        assertEquals("replicated", sendCommand(replicaPort, "GET " + block1Key)));
    }

    // =========================================================
    // Test 6: DELETE on leader
    // =========================================================
    @Test
    @Order(6)
    void deleteOnLeader() throws Exception {
        sendCommand(block1LeaderPort, "PUT " + block1Key + " to-delete");
        assertEquals("to-delete", sendCommand(block1LeaderPort, "GET " + block1Key));

        assertEquals("OK", sendCommand(block1LeaderPort, "DELETE " + block1Key));
        assertEquals("NULL", sendCommand(block1LeaderPort, "GET " + block1Key));
    }

    // =========================================================
    // Test 7: DELETE replicates to replica
    // =========================================================
    @Test
    @Order(7)
    void deleteReplicatesToReplica() throws Exception {
        sendCommand(block1LeaderPort, "PUT " + block1Key + " del-replicate");
        int replicaPort = getBlock1ReplicaPort();

        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(300))
                .ignoreExceptions()
                .untilAsserted(() ->
                        assertEquals("del-replicate", sendCommand(replicaPort, "GET " + block1Key)));

        sendCommand(block1LeaderPort, "DELETE " + block1Key);

        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(300))
                .ignoreExceptions()
                .untilAsserted(() ->
                        assertEquals("NULL", sendCommand(replicaPort, "GET " + block1Key)));
    }

    // =========================================================
    // Test 8: Replica rejects DELETE
    // =========================================================
    @Test
    @Order(8)
    void replicaRejectsDelete() throws Exception {
        int replicaPort = getBlock1ReplicaPort();
        String resp = sendCommand(replicaPort, "DELETE " + block1Key);
        assertTrue(resp.startsWith("ERROR"), "Replica should reject DELETE");
    }

    // =========================================================
    // Test 9: GET nonexistent key returns NULL
    // =========================================================
    @Test
    @Order(9)
    void getNonexistentKey() throws Exception {
        assertEquals("NULL", sendCommand(block1LeaderPort, "GET nonexistent-key-xyz"));
    }

    // =========================================================
    // Test 10: Block-2 leader accepts PUT for block-2 keys
    // =========================================================
    @Test
    @Order(10)
    void block2LeaderAcceptsPut() throws Exception {
        assertEquals("OK", sendCommand(block2LeaderPort, "PUT " + block2Key + " purple"));
        assertEquals("purple", sendCommand(block2LeaderPort, "GET " + block2Key));
    }

    // =========================================================
    // Test 11: Cross-partition fencing
    // =========================================================
    @Test
    @Order(11)
    void crossPartitionFencing() throws Exception {
        String resp = sendCommand(block2LeaderPort, "PUT " + block1Key + " wrong");
        assertTrue(resp.startsWith("ERROR"),
                "Block-2 leader should reject keys owned by block-1, got: " + resp);
    }

    // =========================================================
    // Test 12: Block-2 replication
    // =========================================================
    @Test
    @Order(12)
    void block2Replication() throws Exception {
        sendCommand(block2LeaderPort, "PUT " + block2Key + " b2-replicated");

        int block2ReplicaPort = -1;
        for (int port : BLOCK2_PORTS) {
            if (port != block2LeaderPort) { block2ReplicaPort = port; break; }
        }
        final int replicaPort = block2ReplicaPort;

        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(300))
                .ignoreExceptions()
                .untilAsserted(() ->
                        assertEquals("b2-replicated", sendCommand(replicaPort, "GET " + block2Key)));
    }

    // =========================================================
    // Test 13: Unknown command returns error
    // =========================================================
    @Test
    @Order(13)
    void unknownCommand() throws Exception {
        assertEquals("ERROR: Unknown command", sendCommand(block1LeaderPort, "INVALID foo bar"));
    }

    // =========================================================
    // Test 14: PUT overwrites previous value
    // =========================================================
    @Test
    @Order(14)
    void putOverwrites() throws Exception {
        sendCommand(block1LeaderPort, "PUT " + block1Key + " first");
        assertEquals("first", sendCommand(block1LeaderPort, "GET " + block1Key));

        sendCommand(block1LeaderPort, "PUT " + block1Key + " second");
        assertEquals("second", sendCommand(block1LeaderPort, "GET " + block1Key));
    }

    // =========================================================
    // Test 15: Leader failover
    // =========================================================
    @Test
    @Order(15)
    void leaderFailover() throws Exception {
        sendCommand(block1LeaderPort, "PUT " + block1Key + " before-failover");

        // Map leader port to container name
        String containerName;
        if (block1LeaderPort == NODE1_PORT) containerName = "kvstore-node-1";
        else if (block1LeaderPort == NODE2_PORT) containerName = "kvstore-node-2";
        else containerName = "kvstore-node-5";

        int replicaPort = getBlock1ReplicaPort();

        // Verify replica has the data before failover
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(300))
                .ignoreExceptions()
                .untilAsserted(() ->
                        assertEquals("before-failover", sendCommand(replicaPort, "GET " + block1Key)));

        // Stop the leader container
        new ProcessBuilder("docker", "stop", containerName)
                .directory(new File(System.getProperty("user.dir")))
                .inheritIO().start().waitFor();

        // Wait for ZK session expiry + failover
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2))
                .ignoreExceptions()
                .untilAsserted(() -> {
                    String resp = sendCommand(replicaPort, "PUT " + block1Key + " after-failover");
                    assertEquals("OK", resp, "Replica should accept writes after becoming leader");
                });

        // Verify data after failover
        assertEquals("after-failover", sendCommand(replicaPort, "GET " + block1Key));

        // New leader is fully operational
        String newKey = block1Key + "-new";
        assertEquals("OK", sendCommand(replicaPort, "PUT " + newKey + " new-leader-write"));
        assertEquals("new-leader-write", sendCommand(replicaPort, "GET " + newKey));
    }
}
