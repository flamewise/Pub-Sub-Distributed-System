package com.example.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class BrokerHandler extends Thread {
    private final Socket brokerSocket;
    private final Broker broker;
    private final String brokerAddress;
    private PrintWriter out;

    public BrokerHandler(Socket brokerSocket, Broker broker, String brokerAddress) {
        this.brokerSocket = brokerSocket;
        this.broker = broker;
        this.brokerAddress = brokerAddress;
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(brokerSocket.getInputStream()));
             PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true)) {

            this.out = out;
            handleBrokerCommands(in);

        } catch (IOException e) {
            System.err.println("Broker disconnected abruptly: " + brokerSocket.getInetAddress());
        } finally {
            closeBrokerSocket();
        }
    }

    private void handleBrokerCommands(BufferedReader in) throws IOException {
        // Print the IP address and port of the broker
        String brokerIP = brokerSocket.getInetAddress().getHostAddress();
        int brokerPort = brokerSocket.getPort();
        System.out.println("Handling commands from broker at IP: " + brokerIP + " Port: " + brokerPort);
        System.out.println("Handling commands from broker at IP: " + brokerIP + " Port: " + brokerPort + 
                   ". Full socket info: Local Address: " + brokerSocket.getLocalAddress() + 
                   " Local Port: " + brokerSocket.getLocalPort() + 
                   " Remote Address: " + brokerSocket.getRemoteSocketAddress());


        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            System.out.println("Broker Command: " + inputLine + " from broker at IP: " + brokerIP + " Port: " + brokerPort);
            String[] parts = inputLine.split(" ");

            if (parts.length > 0) {
                String command = parts[0];
                handleCommand(command, parts);
            } else {
                out.println("Invalid broker command.");
            }
        }
    }

    private void handleCommand(String command, String[] parts) {
        try {
            switch (command) {
                case "synchronize_topic":
                    handleSynchronizeTopic(parts);
                    break;
                case "synchronize_message":
                    handleSynchronizeMessage(parts);
                    break;
                case "synchronize_sub":
                    handleSynchronizeSubscription(parts);
                    break;
                default:
                    out.println("Invalid command for broker.");
            }
        } catch (Exception e) {
            out.println("Error processing broker command: " + e.getMessage());
        }
    }

    private void handleSynchronizeTopic(String[] parts) {
        if (parts.length == 3) {
            String topicId = parts[1];
            String topicName = parts[2];
            broker.synchronizeTopic(topicId, topicName);
            out.println("Synchronized topic: " + topicName + " (ID: " + topicId + ")");
        } else {
            out.println("Invalid synchronize_topic message.");
        }
    }

    private void handleSynchronizeMessage(String[] parts) {
        if (parts.length == 3) {
            String topicId = parts[1];
            String message = parts[2];
            broker.synchronizeMessage(topicId, message);
            out.println("Synchronized message to topic: " + topicId);
        } else {
            out.println("Invalid synchronize_message message.");
        }
    }

    private void handleSynchronizeSubscription(String[] parts) {
        if (parts.length == 3) {
            String topicId = parts[1];
            String subscriberId = parts[2];
            broker.synchronizeSubscription(topicId, subscriberId);
            out.println("Synchronized subscription for subscriber: " + subscriberId + " to topic: " + topicId);
        } else {
            out.println("Invalid synchronize_sub message.");
        }
    }

    private void closeBrokerSocket() {
        try {
            if (!brokerSocket.isClosed()) {
                brokerSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
