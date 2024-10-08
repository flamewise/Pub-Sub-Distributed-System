package com.example.broker;

import com.example.subscriber.Subscriber;

import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

public class Broker {
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Subscriber>> topicSubscribers; // topicId -> (username -> Subscriber)
    private final ConcurrentHashMap<String, String> topicNames; // topicId -> topicName
    private final ConcurrentHashMap<String, String> topicPublishers;  // topicId -> publisherUsername
    private final ConcurrentHashMap<String, String> subscriberUsernames;  // Map username to topicId
    private final Set<String> connectedBrokerAddresses = ConcurrentHashMap.newKeySet(); // Stores connected brokers
    private final CopyOnWriteArrayList<Socket> connectedBrokers;
    private final ServerSocket serverSocket;
    private final ExecutorService connectionPool;

    public Broker(int port) throws IOException {
        this.topicSubscribers = new ConcurrentHashMap<>();
        this.topicNames = new ConcurrentHashMap<>();
        this.topicPublishers = new ConcurrentHashMap<>();
        this.subscriberUsernames = new ConcurrentHashMap<>();
        this.connectedBrokers = new CopyOnWriteArrayList<>();
        this.serverSocket = new ServerSocket(port);
        this.connectionPool = Executors.newCachedThreadPool();
        System.out.println("Broker started on port: " + port);
    }

    public void start() {
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    
                // Read the first line to capture both username and connection type
                String inputLine = in.readLine();  // Expecting something like "localhost:12345 broker"
                String[] parts = inputLine.split(" ", 2);  // Split into two parts: username and connection type
    
                if (parts.length == 2) {
                    String username = parts[0];  // First part is username (e.g., "localhost:12345")
                    String connection_type = parts[1];  // Second part is connection type (e.g., "broker")
    
                    System.out.println("Client connected: " + clientSocket.getInetAddress() + " with username: " + username + " with connection type " + connection_type);
    
                    // Create a new ClientHandler and pass the username and connection type
                    connectionPool.submit(new ClientHandler(clientSocket, this, username, connection_type));
    
                    if (connection_type.equals("broker")) {
                        // Get the IP address and port of the connected broker
                        String brokerIP = clientSocket.getInetAddress().getHostAddress();
                        
                        // Instead of clientSocket.getPort(), print the listening port of this broker
                        int brokerPort = serverSocket.getLocalPort();  // This is the port this broker is listening on
                        
                        // Combine the IP and port to form the broker address
                        String brokerAddress = brokerIP + ":" + brokerPort;
                        connectedBrokers.add(clientSocket);
                        connectedBrokerAddresses.add(brokerAddress);
                    }
    
                } else {
                    System.out.println("Invalid connection format.");
                    clientSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    

    public void connectToBroker(String brokerIP, int brokerPort) {
        String brokerAddress = brokerIP + ":" + brokerPort;
    
        if (connectedBrokerAddresses.contains(brokerAddress)) {
            System.out.println("Already connected to broker at: " + brokerAddress);
            return;
        }
    
        connectionPool.submit(() -> {
            try {
                // Connect to the broker
                Socket brokerSocket = new Socket(brokerIP, brokerPort);
                connectedBrokers.add(brokerSocket);
                connectedBrokerAddresses.add(brokerAddress);
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                System.out.println("Connected to Broker at: " + brokerIP + ":" + brokerPort);
    
                // Send "broker" connection type and "localhost:port" as the username
                String localHost = brokerSocket.getLocalAddress().getHostAddress();
                int localPort = brokerSocket.getLocalPort();
                out.println(localHost + ":" + localPort + " broker");
    
                System.out.println("Sent broker_connect message to existing broker at: " + brokerIP + ":" + brokerPort);
    
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
    

    public void createTopic(String username, String topicId, String topicName) {
        if (!topicNames.containsKey(topicId)) {
            topicSubscribers.putIfAbsent(topicId, new ConcurrentHashMap<>());
            topicNames.put(topicId, topicName);
            topicPublishers.put(topicId, username);
            System.out.println(username + " created topic: " + topicName + " (ID: " + topicId + ")");
            synchronizeTopic(topicId, topicName);
        } else {
            System.out.println("Topic already exists: " + topicNames.get(topicId));
        }
    }

    public void removeTopic(String topicId) {
        topicSubscribers.remove(topicId);
        topicNames.remove(topicId);
        topicPublishers.remove(topicId);
        System.out.println("Topic removed: " + topicId);
    }

    public void listAllTopics(PrintWriter out) {
        for (String topicId : topicSubscribers.keySet()) {
            out.println("Topic: " + topicId + " (" + topicNames.get(topicId) + ") by " + topicPublishers.get(topicId));
        }
    }

    public void publishMessage(String username, String topicId, String message) {
        if (topicSubscribers.containsKey(topicId)) {
            ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);

            if (subscribers != null && !subscribers.isEmpty()) {
                for (Subscriber subscriber : subscribers.values()) {
                    subscriber.receiveMessage(topicId, message);
                }
            }
            synchronizeMessage(topicId, message);
        } else {
            System.out.println("Topic not found: " + topicId);
        }
    }

    public void addSubscriber(String topicId, Subscriber subscriber, String username) {
        ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.computeIfAbsent(topicId, k -> new ConcurrentHashMap<>());
        if (!subscribers.containsKey(username)) {
            subscribers.put(username, subscriber);
            subscriberUsernames.put(username, topicId);
            synchronizeSubscription(topicId, subscriber.getUsername());
        }
    }

    public void removeSubscriber(String topicId, Subscriber subscriber) {
        ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);
        if (subscribers != null) {
            subscribers.values().remove(subscriber);
        }
    }

    public void unsubscribe(String topicId, String username) {
        ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);
        if (subscribers != null && subscribers.containsKey(username)) {
            subscribers.remove(username);
            subscriberUsernames.remove(username);
        }
    }

    public int getSubscriberCount(String topicId) {
        ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);
        return (subscribers != null) ? subscribers.size() : 0;
    }

    public void listSubscriptions(PrintWriter out, String subscriberId) {
        for (String topicId : topicSubscribers.keySet()) {
            ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);
            if (subscribers.containsKey(subscriberId)) {
                out.println("Subscribed to: " + topicId + " (" + topicNames.get(topicId) + ")");
            }
        }
    }

    public void synchronizeTopic(String topicId, String topicName) {
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("synchronize_topic " + topicId + " " + topicName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void synchronizeMessage(String topicId, String message) {
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("synchronize_message " + topicId + " " + message);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void synchronizeSubscription(String topicId, String subscriberId) {
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("synchronize_sub " + topicId + " " + subscriberId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void requestTopicFromBrokers(String topicId) {
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("request_topic " + topicId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Set<String> getConnectedBrokerAddresses() {
        return connectedBrokerAddresses;
    }
}
