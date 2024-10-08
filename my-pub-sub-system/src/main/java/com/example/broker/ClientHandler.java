package com.example.broker;

import com.example.subscriber.Subscriber;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler extends Thread {
    private final Socket clientSocket;
    private final Broker broker;
    private final String username;  // Captured from the first line sent by the client
    private final String connectionType;  // Stores the type of connection (publisher, subscriber, or broker)
    private PrintWriter out;

    public ClientHandler(Socket socket, Broker broker, String username, String connectionType) {
        this.clientSocket = socket;
        this.broker = broker;
        this.username = username;
        this.connectionType = connectionType;
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
            String[] parts = inputLine.split(" ", 3);  // Now expects commands with topicId and message as needed

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
            switch (connectionType) {
                case "publisher":
                    handlePublisherCommands(command, parts);
                    break;
                case "subscriber":
                    handleSubscriberCommands(command, parts);
                    break;
                case "broker":
                    handleBrokerCommands(command, parts);
                    break;
                default:
                    out.println("Unknown connection type.");
            }
        } catch (Exception e) {
            out.println("Error processing command: " + e.getMessage());
        }
    }

    private void handlePublisherCommands(String command, String[] parts) {
        switch (command) {
            case "create":
                handleCreate(parts);
                break;
            case "publish":
                handlePublish(parts);
                break;
            default:
                out.println("Invalid command for publisher.");
        }
    }

    private void handleSubscriberCommands(String command, String[] parts) {
        switch (command) {
            case "sub":
                handleSubscribe(parts);
                break;
            case "unsub":
                handleUnsubscribe(parts);
                break;
            case "current":
                handleCurrent(parts);
                break;
            default:
                out.println("Invalid command for subscriber.");
        }
    }

    private void handleBrokerCommands(String command, String[] parts) {
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
            case "request_topic":
                handleRequestTopic(parts);
                break;
            default:
                out.println("Invalid command for broker.");
        }
    }

    private void handleCreate(String[] parts) {
        if (parts.length == 3) {
            broker.createTopic(username, parts[1], parts[2]);
            out.println("Topic created: " + parts[2] + " (ID: " + parts[1] + ")");
        } else {
            out.println("Usage: create {topic_id} {topic_name}");
        }
    }

    private void handlePublish(String[] parts) {
        if (parts.length == 3) {
            broker.publishMessage(username, parts[1], parts[2], true);
            out.println("Message published to topic: " + parts[1]);
        } else {
            out.println("Usage: publish {topic_id} {message}");
        }
    }

    private void handleSubscribe(String[] parts) {
        if (parts.length == 2) {
            Subscriber subscriber = new Subscriber(username, out);
            broker.addSubscriber(parts[1], subscriber, username);
            out.println(username + " subscribed to topic: " + parts[1]);
        } else {
            out.println("Usage: sub {topic_id}");
        }
    }

    private void handleUnsubscribe(String[] parts) {
        if (parts.length == 2) {
            broker.unsubscribe(parts[1], username);
            out.println(username + " unsubscribed from topic: " + parts[1]);
        } else {
            out.println("Usage: unsub {topic_id}");
        }
    }

    private void handleCurrent(String[] parts) {
        broker.listSubscriptions(out, username);
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

    private void handleRequestTopic(String[] parts) {
        if (parts.length == 2) {
            String topicId = parts[1];
            broker.requestTopicFromBrokers(topicId);
        } else {
            out.println("Invalid request_topic message.");
        }
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
