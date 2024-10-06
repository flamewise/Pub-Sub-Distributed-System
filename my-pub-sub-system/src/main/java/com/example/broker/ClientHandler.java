package com.example.broker;

import com.example.subscriber.SubscriberImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler extends Thread {
    private final Socket clientSocket;
    private final Broker broker;
    private final String subscriberId;
    private PrintWriter out;

    public ClientHandler(Socket socket, Broker broker) {
        this.clientSocket = socket;
        this.broker = broker;
        this.subscriberId = "client_" + clientSocket.getPort();  // Unique ID for each subscriber
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            this.out = out;
            handleClientCommands(in);

        } catch (IOException e) {
            System.err.println("Client disconnected abruptly: " + clientSocket.getInetAddress());
        } finally {
            closeClientSocket();
        }
    }

    private void handleClientCommands(BufferedReader in) throws IOException {
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            System.out.println("Received: " + inputLine);
            String[] parts = inputLine.split(" ", 3);

            if (parts.length > 0) {
                String command = parts[0];
                handleCommand(command, parts);
            } else {
                out.println("Invalid command.");
            }
        }
    }

    private void handleCommand(String command, String[] parts) {
        try {
            switch (command) {
                case "broker_connect": // Add this case to handle incoming broker connections
                    handleBrokerConnect(parts);
                    break;
                case "create":
                    handleCreate(parts);
                    break;
                case "publish":
                    handlePublish(parts);
                    break;
                case "show":
                    handleShow(parts);
                    break;
                case "delete":
                    handleDelete(parts);
                    break;
                case "list":
                    handleList(parts);
                    break;
                case "sub":
                    handleSubscribe(parts);
                    break;
                case "current":
                    handleCurrentSubscriptions();
                    break;
                case "unsub":
                    handleUnsubscribe(parts);
                    break;
                case "exit":
                    handleExit();
                    break;
                case "synchronize_message":
                    handleSynchronizeMessage(parts);
                    break;
                default:
                    out.println("Unknown command. Please try again.");
            }
        } catch (Exception e) {
            out.println("Error processing command: " + e.getMessage());
        }
    }

    // Handle broker connections by reverse connecting back to the initiating broker
    private void handleBrokerConnect(String[] parts) {
        if (parts.length == 3) {
            String brokerIP = parts[1];
            int brokerPort = Integer.parseInt(parts[2]);
            
            // Check if this broker is already connected
            if (!broker.getConnectedBrokerAddresses().contains(brokerIP + ":" + brokerPort)) {
                // If not connected, establish a connection back to the broker
                broker.connectToBroker(brokerIP, brokerPort);
                System.out.println("Connecting back to broker at " + brokerIP + ":" + brokerPort);
            } else {
                System.out.println("Already connected to broker at " + brokerIP + ":" + brokerPort);
            }
        } else {
            System.out.println("Invalid broker connection message.");
        }
    }

    private void handleCreate(String[] parts) {
        if (parts.length == 3) {
            broker.createTopic(parts[1], parts[2]);
            out.println("Topic created: " + parts[2] + " (ID: " + parts[1] + ")");
        } else {
            out.println("Usage: create {topic_id} {topic_name}");
        }
    }

    private void handlePublish(String[] parts) {
        if (parts.length == 3) {
            broker.publishMessage(parts[1], parts[2]);
            out.println("Message published to topic: " + parts[1]);
        } else {
            out.println("Usage: publish {topic_id} {message}");
        }
    }

    private void handleShow(String[] parts) {
        if (parts.length == 2) {
            int count = broker.getSubscriberCount(parts[1]);
            out.println("Topic: " + parts[1] + " has " + count + " subscribers.");
        } else {
            out.println("Usage: show {topic_id}");
        }
    }

    private void handleDelete(String[] parts) {
        if (parts.length == 2) {
            broker.removeTopic(parts[1]);
            out.println("Topic deleted: " + parts[1]);
        } else {
            out.println("Usage: delete {topic_id}");
        }
    }

    private void handleList(String[] parts) {
        if (parts.length == 2 && "all".equals(parts[1])) {
            broker.listAllTopics(out);
        } else {
            out.println("Usage: list all");
        }
    }

    private void handleSubscribe(String[] parts) {
        if (parts.length == 2) {
            SubscriberImpl subscriber = new SubscriberImpl(subscriberId, out);
            broker.addSubscriber(parts[1], subscriber);
            out.println("Subscribed to topic: " + parts[1]);
        } else {
            out.println("Usage: sub {topic_id}");
        }
    }

    private void handleCurrentSubscriptions() {
        broker.listSubscriptions(out, subscriberId);
    }

    private void handleUnsubscribe(String[] parts) {
        if (parts.length == 2) {
            broker.unsubscribe(parts[1], subscriberId);
            out.println("Unsubscribed from topic: " + parts[1]);
        } else {
            out.println("Usage: unsub {topic_id}");
        }
    }

    private void handleSynchronizeMessage(String[] parts) {
        if (parts.length == 3) {
            String topicId = parts[1];
            String message = parts[2];
            
            // Deliver the message to local subscribers on this broker
            broker.publishMessageToLocalSubscribers(topicId, message);  // Only send to local subscribers
        }
    }

    private void handleExit() throws IOException {
        out.println("Closing connection...");
        clientSocket.close();
    }

    private void closeClientSocket() {
        try {
            if (!clientSocket.isClosed()) {
                clientSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}