package com.db.memory.server;

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
    private volatile boolean running = true;

    public KVServer(int port, Map<String, String> store, ReplicationManager replicationManager) {
        this.port = port;
        this.store = store;
        this.replicationManager = replicationManager;
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("[KVServer] Listening on port " + port);
            while (running) {
                Socket client = serverSocket.accept();
                new Thread(() -> handleClient(client)).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleClient(Socket client) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
             PrintWriter writer = new PrintWriter(client.getOutputStream(), true)) {

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ", 3);
                if (parts.length >= 2) {
                    String command = parts[0].toUpperCase();
                    String key = parts[1];

                    switch (command) {
                        case "PUT":
                            if (parts.length == 3) {
                                store.put(key, parts[2]);
                                replicationManager.replicatePut(key, parts[2]);
                                writer.println("OK");
                                System.out.println("[KVServer] PUT key=" + key + ", value=" + parts[2]);
                            } else {
                                writer.println("ERROR: PUT requires key and value");
                            }
                            break;
                        case "GET":
                            String value = store.get(key);
                            writer.println(value != null ? value : "NOT_FOUND");
                            System.out.println("[KVServer] GET key=" + key + ", value=" + value);
                            break;
                        case "DELETE":
                            store.remove(key);
                            replicationManager.replicateDelete(key);
                            writer.println("OK");
                            System.out.println("[KVServer] DELETE key=" + key);
                            break;
                        case "REPLICATE_PUT":
                            if (parts.length == 3) {
                                store.put(key, parts[2]);
                                writer.println("REPLICATED");
                                System.out.println("[KVServer] REPLICATE_PUT key=" + key + ", value=" + parts[2]);
                            }
                            break;
                        case "REPLICATE_DELETE":
                            store.remove(key);
                            writer.println("REPLICATED");
                            System.out.println("[KVServer] REPLICATE_DELETE key=" + key);
                            break;
                        default:
                            writer.println("ERROR: Unknown command");
                            System.out.println("[KVServer] Unknown command: " + command);
                    }
                } else {
                    writer.println("ERROR: Invalid command format");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        running = false;
    }
}
