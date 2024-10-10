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
        //System.out.println("Client Connect"+username + " " + connectionType);
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
        // Print the IP address and port of the client
        String clientIP = clientSocket.getInetAddress().getHostAddress();
        int clientPort = clientSocket.getPort();
        System.out.println("Handling commands from client IP: " + clientIP + " Port: " + clientPort + " with type " + connectionType);
    
        String inputLine;
        while (true) {
            inputLine = in.readLine();
            System.out.println("Client Command: " + inputLine + " from " + connectionType + " at IP: " + clientIP + " Port: " + clientPort);
            String[] parts = inputLine.split(" ");
    
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
        System.out.println("Subscriber" + command);
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
            case "list_all":
                // New case to handle topic listing
                broker.listAllTopics(out);
                break;
            default:
                out.println("Invalid command for subscriber.");
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
            // Call the broker's method to publish the message and synchronize it across brokers
            broker.publishMessage(parts[1], parts[2], true); // `true` means synchronization is needed
            out.println("Message published to topic: " + parts[1]);
        } else {
            out.println("Usage: publish {topic_id} {message}");
        }
    }
    

    private void handleSubscribe(String[] parts) {
        if (parts.length == 2) {
            Subscriber subscriber = new Subscriber(username, out);
            broker.addSubscriber(parts[1], subscriber, username, true);
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
