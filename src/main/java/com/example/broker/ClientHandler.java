/**
 * Name: Simon Chen
 * Surname: Chen
 * Student ID: 1196439
 *
 * Description: The ClientHandler class manages the connection between a client (publisher or subscriber) and
 * the broker. It handles client commands, such as publishing messages, subscribing to topics, and unsubscribing.
 * The class ensures that clients can communicate effectively with the broker.
 * 
 * Date: 11/10/2024
 */
package com.example.broker;

import com.example.subscriber.Subscriber;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class ClientHandler extends Thread {
    private final Socket clientSocket;
    private final Broker broker;
    private final String username;  // Captured from the first line sent by the client
    private final String connectionType;  // Stores the type of connection (publisher, subscriber, or broker)
    private PrintWriter out;
    private BufferedReader in;

    public ClientHandler(Socket socket, Broker broker, String username, String connectionType) {
        this.clientSocket = socket;
        this.broker = broker;
        this.username = username;
        this.connectionType = connectionType;
        
        try {
            // Initialize BufferedReader and PrintWriter from clientSocket's input/output streams
            this.in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            this.out = new PrintWriter(clientSocket.getOutputStream(), true);
        } catch (IOException e) {
            System.err.println("Error initializing input/output streams for client: " + e.getMessage());
        }
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
    
            // Check if the client has disconnected
            if (inputLine == null) {
                System.out.println("Client disconnected: " + clientIP + ":" + clientPort);
                if (connectionType.equals("publisher")) {
                    handlePublisherCrash();
                } else {
                    handleSubscriberCrash();
                }
                break;  // Exit the loop when the client disconnects
            }
    
            //System.out.println("Client Command: " + inputLine + " from " + connectionType + " at IP: " + clientIP + " Port: " + clientPort);
            String[] parts = inputLine.split(" ");
    
            if (parts.length > 0) {
                String command = parts[0];
                handleCommand(command, parts);
            } else {
                out.println("error: Invalid command.");
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
                    out.println("error: Unknown connection type.");
            }
        } catch (Exception e) {
            out.println("error: Error processing command: " + e.getMessage());
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
            case "show":
                handleShow(parts);
                break;
            case "delete":
                handleDelete(parts);
                break;
            default:
                out.println("error: Invalid command for publisher.");
        }
    }
    
    private void handleDelete(String[] parts) {
        if (parts.length == 2) {
            String topicId = parts[1];
            
            // Check if the topic exists
            if (!broker.topicExists(topicId)) {
                out.println("error: Topic with ID " + topicId + " does not exist.");
            } else {
                // Check if the current user is the owner of the topic
                if (!broker.isTopicOwner(topicId, username)) {
                    out.println("error: You do not have permission to delete this topic. Only the owner can delete it.");
                } else {
                    // Call the broker's deleteTopic method to delete the topic and notify subscribers
                    broker.deleteTopic(topicId, true);  // `true` ensures the deletion is synchronized with other brokers
                    out.println("success: Topic " + topicId + " has been deleted.");
                }
            }
        } else {
            out.println("error: Usage: delete {topic_id}");
        }
    }
         

    private void handleShow(String[] parts) {
        if (parts.length == 2) {
            // Call the broker's method to show the subscriber count for a given topic
            broker.showSubscriberCount(parts[1], out);
        } else {
            out.println("error: Usage: show {topic_id}");
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
                handleListAll();
                break;
            default:
                out.println("error: Invalid command for subscriber.");
        }
    }



    private void handleCreate(String[] parts) {
        if (parts.length == 3) {
            String topicId = parts[1];
            
            // Check if the topic already exists
            if (broker.topicExists(topicId)) {
                out.println("error: Topic with ID " + topicId + " already exists.");
            } else {
                // Create the topic if it doesn't exist
                broker.createTopic(username, topicId, parts[2]);
                out.println("success: " + "Topic created: " + parts[2] + " (ID: " + topicId + ")");
            }
        } else {
            out.println("error: Usage: create {topic_id} {topic_name}");
        }
    }
    

    private void handleListAll() {
        broker.listAllTopics(out);
    }

    private void handlePublish(String[] parts) {
        if (parts.length >= 3) {
            String topicId = parts[1];
    
            // Check if the topic exists in the broker before publishing
            if (!broker.topicExists(topicId)) {
                out.println("error: Topic " + topicId + " does not exist.");
                return;
            }
    
            // Check if the current user is the owner of the topic
            if (!broker.isTopicOwner(topicId, username)) {
                out.println("error: You are not the owner of topic " + topicId + ".");
                return;
            }
    
            // Combine all elements from parts[2] onward into a single message string
            StringBuilder messageBuilder = new StringBuilder(parts[2]);
            for (int i = 3; i < parts.length; i++) {
                messageBuilder.append(" ").append(parts[i]);
            }
            String message = messageBuilder.toString();
            
            // Call the broker's method to publish the message and synchronize it across brokers
            broker.publishMessage(topicId, message, true); // `true` means synchronization is needed
            
            // Generate timestamp and confirm successful publishing
            String timestamp = new java.text.SimpleDateFormat("dd/MM HH:mm:ss").format(new java.util.Date());
            out.println(timestamp + " " + topicId + ":" + broker.topicNames.get(topicId) + ": " + "Message published to topic: " + topicId);
        } else {
            out.println("error: Usage: publish {topic_id} {message}");
        }
    }
    
    
    private void handleSubscribe(String[] parts) {
        if (parts.length == 2) {
            String topicId = parts[1];
    
            // Check if the topic exists in the broker
            if (!broker.topicExists(topicId)) {
                out.println("error: Topic " + topicId + " does not exist.");
                return;
            }
    
            // Check if the user is already subscribed to the topic
            if (broker.isSubscribed(topicId, username)) {
                out.println("error: " + username + " is already subscribed to topic: " + topicId);
            } else {
                // Add the subscriber ID (username) directly to the broker without creating a Subscriber object
                broker.addSubscriberId(topicId, username, true);
                out.println("success: " + username + " subscribed to topic: " + topicId);
            }
        } else {
            out.println("error: Usage: sub {topic_id}");
        }
    }
    
    
    
    private void handleUnsubscribe(String[] parts) {
        if (parts.length == 2) {
            String topicId = parts[1];
    
            // Check if the user is actually subscribed to the topic
            if (!broker.isSubscribed(topicId, username)) {
                out.println("error: " + username + " is not subscribed to topic: " + topicId);
            } else {
                // Call the broker's unsubscribe method with synchronization set to true
                broker.unsubscribe(topicId, username, true);
                out.println("success: " + username + " unsubscribed from topic: " + topicId);
            }
        } else {
            out.println("error: Usage: unsub {topic_id}");
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

    public String getConnectionType() {
        return connectionType;
    }
    
    public Socket getClientSocket() {
        return clientSocket;
    }
    
    public String getUserName(){
        return username;
    }

    private void handlePublisherCrash() {
        System.out.println("Handling publisher crash for: " + username);
    
        // Remove the publisher's ClientHandler from pubClientHandlers
        broker.removePublisherClientHandler(username);
    
        // Iterate over all topics to find the ones created by this publisher
        for (String topicId : broker.topicPublishers.keySet()) {
            String publisher = broker.topicPublishers.get(topicId);
            if (publisher.equals(username)) {
                // Delete the topic and notify all subscribers
                broker.deleteTopic(topicId, true); // true indicates that synchronization is required
                System.out.println("Deleted topic " + topicId + " due to publisher crash: " + username);
            }
        }
    }
    
    private void handleSubscriberCrash() {
        System.out.println("Handling subscriber crash for: " + username);

        // Remove the publisher's ClientHandler from pubClientHandlers
        broker.removeSubscriberClientHandler(username);
    
        // Iterate over all topics to find the subscriptions of this subscriber
        for (String topicId : broker.topicSubscribers.keySet()) {
            ConcurrentHashMap<String, Subscriber> subscribers = broker.topicSubscribers.get(topicId);
            if (subscribers != null && subscribers.containsKey(username)) {
                // Unsubscribe the subscriber from this topic
                broker.unsubscribe(topicId, username, true); // true indicates that synchronization is required
                System.out.println("Unsubscribed " + username + " from topic " + topicId + " due to subscriber crash");
            }
        }
    }
    

}
