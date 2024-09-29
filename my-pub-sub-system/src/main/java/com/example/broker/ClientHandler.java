package com.example.broker;

import com.example.subscriber.SubscriberImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler extends Thread {
    private Socket clientSocket;
    private Broker broker;
    private String subscriberId;
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

            this.out = out; // Save the output stream for sending messages

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                System.out.println("Received: " + inputLine);

                String[] parts = inputLine.split(" ", 3);  // Split into three parts: command, topic_id, topic_name/message
                String command = parts[0];

                try {
                    switch (command) {
                        case "create":
                            if (parts.length == 3) {
                                String topicId = parts[1];
                                String topicName = parts[2];
                                broker.createTopic(topicId, topicName);  // Pass both topicId and topicName to Broker
                                out.println("Topic created: " + topicName + " (ID: " + topicId + ")");
                            } else {
                                out.println("Usage: create {topic_id} {topic_name}");
                            }
                            break;

                        case "publish":
                            if (parts.length == 3) {
                                String topicId = parts[1];
                                String message = parts[2];
                                broker.publishMessage(topicId, message);
                                out.println("Message published to " + topicId);
                            } else {
                                out.println("Usage: publish {topic_id} {message}");
                            }
                            break;

                        case "show":
                            if (parts.length == 2) {
                                String topicId = parts[1];
                                int count = broker.getSubscriberCount(topicId);
                                out.println("Topic: " + topicId + " has " + count + " subscribers.");
                            } else {
                                out.println("Usage: show {topic_id}");
                            }
                            break;

                        case "delete":
                            if (parts.length == 2) {
                                String topicId = parts[1];
                                broker.removeTopic(topicId);
                                out.println("Topic deleted: " + topicId);
                            } else {
                                out.println("Usage: delete {topic_id}");
                            }
                            break;

                        case "list":
                            if (parts.length == 2 && parts[1].equals("all")) {
                                broker.listAllTopics(out);
                            } else {
                                out.println("Usage: list all");
                            }
                            break;

                        case "sub":
                            if (parts.length == 2) {
                                String topicId = parts[1];
                                SubscriberImpl subscriber = new SubscriberImpl(subscriberId, out);  // Create a new subscriber instance
                                broker.addSubscriber(topicId, subscriber);  // Add the subscriber to the broker
                                out.println("Subscribed to: " + topicId);
                            } else {
                                out.println("Usage: sub {topic_id}");
                            }
                            break;

                        case "current":
                            broker.listSubscriptions(out, subscriberId);  // List current subscriptions for this subscriber
                            break;

                        case "unsub":
                            if (parts.length == 2) {
                                String topicId = parts[1];
                                broker.unsubscribe(topicId, subscriberId);
                                out.println("Unsubscribed from: " + topicId);
                            } else {
                                out.println("Usage: unsub {topic_id}");
                            }
                            break;

                        case "exit":
                            out.println("Closing connection...");
                            clientSocket.close();
                            return;  // Exit the while loop

                        // Handle synchronization messages from other brokers
                        case "synchronize_topic":
                            if (parts.length == 3) {
                                String topicId = parts[1];
                                String topicName = parts[2];
                                broker.createTopic(topicId, topicName);  // Ensure the broker creates the topic with both ID and name
                                System.out.println("Synchronized topic " + topicName + " (ID: " + topicId + ")");
                            }
                            break;

                        case "synchronize_sub":
                            if (parts.length == 3) {
                                String topicId = parts[1];
                                String subscriberId = parts[2];
                                broker.addSubscriber(topicId, new SubscriberImpl(subscriberId, out));  // Add subscriber on other brokers
                                System.out.println("Synchronized subscription for topic " + topicId + " from remote subscriber " + subscriberId);
                            }
                            break;

                        case "synchronize_message":
                            if (parts.length == 3) {
                                String topicId = parts[1];
                                String message = parts[2];
                                broker.publishMessage(topicId, message);  // Synchronize and publish message to local subscribers
                            }
                            break;

                        default:
                            out.println("Unknown command. Please try again.");
                            break;
                    }
                } catch (Exception e) {
                    out.println("Error processing command: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            System.err.println("Client disconnected abruptly: " + clientSocket.getInetAddress());
        } finally {
            try {
                if (clientSocket != null && !clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
