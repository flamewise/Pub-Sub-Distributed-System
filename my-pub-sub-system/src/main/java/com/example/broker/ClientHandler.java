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
            case "show":
                handleShow(parts);
                break;
            case "delete":
                handleDelete(parts);
                break;
            default:
                out.println("Invalid command for publisher.");
        }
    }
    
    private void handleDelete(String[] parts) {
        if (parts.length == 2) {
            String topicId = parts[1];
    
            // Call the broker's deleteTopic method to delete the topic and notify subscribers
            broker.deleteTopic(topicId, true);  // `true` ensures the deletion is synchronized with other brokers
            
            out.println("Topic " + topicId + " has been deleted.");
        } else {
            out.println("Usage: delete {topic_id}");
        }
    }      

    private void handleShow(String[] parts) {
        if (parts.length == 2) {
            // Call the broker's method to show the subscriber count for a given topic
            broker.showSubscriberCount(parts[1], out);
        } else {
            out.println("Usage: show {topic_id}");
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
                out.println("Invalid command for subscriber.");
        }
    }



    private void handleCreate(String[] parts) {
        if (parts.length == 3) {
            broker.createTopic(username, parts[1], parts[2]);
            String topicId = parts[1];
            String topicName = parts[2];
            String timestamp = new java.text.SimpleDateFormat("dd/MM HH:mm:ss").format(new java.util.Date());
            out.println(timestamp + " " + topicId + ":" + topicName + ":" + "Topic created: " + parts[2] + " (ID: " + parts[1] + ")");
        } else {
            out.println("Usage: create {topic_id} {topic_name}");
        }
    }

    private void handleListAll() {
        broker.listAllTopics(out);
    }

    private void handlePublish(String[] parts) {
        if (parts.length == 3) {
            // Call the broker's method to publish the message and synchronize it across brokers
            broker.publishMessage(parts[1], parts[2], true); // `true` means synchronization is needed
            String timestamp = new java.text.SimpleDateFormat("dd/MM HH:mm:ss").format(new java.util.Date());
            out.println(timestamp + " " + parts[1] + ":" + broker.topicNames.get(parts[1]) + ": " + "Message published to topic: " + parts[1]);
        } else {
            out.println("Usage: publish {topic_id} {message}");
        }
    }
    

    private void handleSubscribe(String[] parts) {
        if (parts.length == 2) {
            String topicId = parts[1];
            // Add the subscriber ID (username) directly to the broker without creating a Subscriber object
            broker.addSubscriberId(topicId, username, true);
    
            // Get the current date and time in the desired format: dd/MM HH:mm:ss
            String timestamp = new java.text.SimpleDateFormat("dd/MM HH:mm:ss").format(new java.util.Date());
    
            // Send subscription confirmation with date info
            System.out.println(parts[0]+ " " + parts[1] + "qwdqwwd");
            out.println(timestamp + " " + topicId + ":" + broker.topicNames.get(topicId) + ":" + username + " subscribed to topic: " + topicId);
        } else {
            out.println("Usage: sub {topic_id}");
        }
    }
    
    private void handleUnsubscribe(String[] parts) {
        if (parts.length == 2) {
            String topicId = parts[1];
    
            // Call the broker's unsubscribe method with synchronization set to true
            broker.unsubscribe(topicId, username, true);
    
            // Get the current date and time in the desired format: dd/MM HH:mm:ss
            String timestamp = new java.text.SimpleDateFormat("dd/MM HH:mm:ss").format(new java.util.Date());
    
            // Send unsubscription confirmation with date info
            out.println(timestamp + " " + topicId + ":" + broker.topicNames.get(topicId) + ":" + username + " unsubscribed from topic: " + topicId);
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

    public String getConnectionType() {
        return connectionType;
    }
    
    public Socket getClientSocket() {
        return clientSocket;
    }
    
    public String getUserName(){
        return username;
    }


}
