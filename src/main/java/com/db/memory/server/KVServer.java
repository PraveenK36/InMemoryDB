package com.db.memory.server;

import com.db.memory.cluster.ClusterManager;
import com.db.memory.hashing.HashRing;
import com.db.memory.replication.ReplicationManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

public class KVServer implements Runnable {
    private final int port;
    private final Map<String, String> store;
    private final ReplicationManager replicationManager;
    private final ClusterManager clusterManager;
    private final HashRing hashRing;

    public KVServer(int port, Map<String, String> store, ReplicationManager replicationManager,
                    ClusterManager clusterManager, HashRing hashRing) {
        this.port = port;
        this.store = store;
        this.replicationManager = replicationManager;
        this.clusterManager = clusterManager;
        this.hashRing = hashRing;
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("[KVServer] Listening on port " + port);
            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(() -> handleClient(socket)).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleClient(Socket socket) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

            String line = reader.readLine();
            if (line == null) return;

            String[] parts = line.split(" ");
            String command = parts[0];
            String key = parts.length > 1 ? parts[1] : null;
            String value = parts.length > 2 ? parts[2] : null;

            String expectedOwner = hashRing.getTargetNode(key);
            String currentLeader = clusterManager.getCurrentLeader(expectedOwner);
            boolean isLeader = clusterManager.getNodeId().equals(currentLeader);

            if (("PUT".equalsIgnoreCase(command) || "DELETE".equalsIgnoreCase(command)) && !isLeader) {
                writer.println("ERROR: Node " + clusterManager.getNodeId() + " is not the leader for key " + key);
                return;
            }

            switch (command.toUpperCase()) {
                case "PUT" -> {
                    store.put(key, value);
                    replicationManager.replicatePut(key, value);
                    writer.println("OK");
                }
                case "GET" -> {
                    String result = store.getOrDefault(key, "NULL");
                    writer.println(result);
                }
                case "DELETE" -> {
                    store.remove(key);
                    replicationManager.replicateDelete(key);
                    writer.println("OK");
                }
                default -> writer.println("ERROR: Unknown command");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
