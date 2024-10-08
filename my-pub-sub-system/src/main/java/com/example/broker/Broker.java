package com.example.broker;

import com.example.subscriber.Subscriber;
import com.example.directory.DirectoryServiceClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Broker {
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Subscriber>> topicSubscribers; // topicId -> (username -> Subscriber)
    private final ConcurrentHashMap<String, String> topicNames; // topicId -> topicName
    private final ConcurrentHashMap<String, String> topicPublishers;  // topicId -> publisherUsername
    private final ConcurrentHashMap<String, String> subscriberUsernames;  // Map username to topicId
    private final Set<String> connectedBrokerAddresses = ConcurrentHashMap.newKeySet(); // Stores connected brokers
    private final CopyOnWriteArrayList<Socket> connectedBrokers;
    private final ExecutorService connectionPool;
    private final DirectoryServiceClient directoryServiceClient;
    private final ServerSocket serverSocket;

    public Broker(int port, String directoryServiceAddress) throws IOException {
        this.topicSubscribers = new ConcurrentHashMap<>();
        this.topicNames = new ConcurrentHashMap<>();
        this.topicPublishers = new ConcurrentHashMap<>();
        this.subscriberUsernames = new ConcurrentHashMap<>();
        this.connectedBrokers = new CopyOnWriteArrayList<>();
        this.connectionPool = Executors.newCachedThreadPool();
        this.directoryServiceClient = new DirectoryServiceClient(directoryServiceAddress);
        this.serverSocket = new ServerSocket(port);
        System.out.println("Broker started on port: " + port);

        // Register the broker with the directory service
        String brokerAddress = serverSocket.getInetAddress().getHostAddress() + ":" + port;
        directoryServiceClient.registerBroker(brokerAddress);
        System.out.println("Broker registered with Directory Service at: " + directoryServiceAddress);
    }

    // Broker start method to listen for incoming connections
    public void start() {
        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                
                // Read the first message to capture username and connectionType
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String firstMessage = in.readLine();
                String[] parts = firstMessage.split(" ");
                if (parts.length == 2) {
                    String username = parts[0];  // Capture the username
                    String connectionType = parts[1];  // Capture the connection type (publisher, subscriber, or broker)
                    
                    // Pass username and connectionType to ClientHandler
                    connectionPool.submit(new ClientHandler(clientSocket, this, username, connectionType));
                } else {
                    System.out.println("Invalid first message format.");
                    clientSocket.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTopic(String username, String topicId, String topicName) {
        if (!topicNames.containsKey(topicId)) {
            topicSubscribers.putIfAbsent(topicId, new ConcurrentHashMap<>());
            topicNames.put(topicId, topicName);
            topicPublishers.put(topicId, username);
            System.out.println(username + " created topic: " + topicName + " (ID: " + topicId + ")");
        } else {
            System.out.println("Topic already exists: " + topicNames.get(topicId));
        }
    }

    public void publishMessage(String username, String topicId, String message, boolean synchronizedRequired) {
        if (topicSubscribers.containsKey(topicId)) {
            ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);

            if (subscribers != null && !subscribers.isEmpty()) {
                for (Subscriber subscriber : subscribers.values()) {
                    subscriber.receiveMessage(topicId, message);
                }
            }

            if (synchronizedRequired) {
                synchronizeMessage(topicId, message);
            }

        } else {
            System.out.println("Topic not found: " + topicId);
        }
    }

    public void addSubscriber(String topicId, Subscriber subscriber, String username) {
        ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.computeIfAbsent(topicId, k -> new ConcurrentHashMap<>());
        if (!subscribers.containsKey(username)) {
            subscribers.put(username, subscriber);
            subscriberUsernames.put(username, topicId);
        }
    }

    public void unsubscribe(String topicId, String username) {
        ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);
        if (subscribers != null && subscribers.containsKey(username)) {
            subscribers.remove(username);
            subscriberUsernames.remove(username);
        }
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
        Set<String> activeBrokers = directoryServiceClient.getActiveBrokers();
        for (String brokerAddress : activeBrokers) {
            if (!connectedBrokerAddresses.contains(brokerAddress)) {
                connectToBroker(brokerAddress);
            }
        }

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
        Set<String> activeBrokers = directoryServiceClient.getActiveBrokers();
        for (String brokerAddress : activeBrokers) {
            if (!connectedBrokerAddresses.contains(brokerAddress)) {
                connectToBroker(brokerAddress);
            }
        }

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
        Set<String> activeBrokers = directoryServiceClient.getActiveBrokers();
        for (String brokerAddress : activeBrokers) {
            if (!connectedBrokerAddresses.contains(brokerAddress)) {
                connectToBroker(brokerAddress);
            }
        }

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
        Set<String> activeBrokers = directoryServiceClient.getActiveBrokers();
        for (String brokerAddress : activeBrokers) {
            if (!connectedBrokerAddresses.contains(brokerAddress)) {
                connectToBroker(brokerAddress);
            }
        }

        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("request_topic " + topicId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void connectToBroker(String brokerAddress) {
        String[] parts = brokerAddress.split(":");
        String brokerIP = parts[0];
        int brokerPort = Integer.parseInt(parts[1]);

        if (connectedBrokerAddresses.contains(brokerAddress)) {
            System.out.println("Already connected to broker at: " + brokerAddress);
            return;
        }

        connectionPool.submit(() -> {
            try {
                Socket brokerSocket = new Socket(brokerIP, brokerPort);
                connectedBrokers.add(brokerSocket);
                connectedBrokerAddresses.add(brokerAddress);
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                System.out.println("Connected to Broker at: " + brokerIP + ":" + brokerPort);

                connectionPool.submit(new ClientHandler(brokerSocket, this, brokerIP + ":" + brokerPort, "broker"));
            } catch (IOException e) {
                System.out.println("Error connecting to broker at " + brokerAddress + ": " + e.getMessage());
            }
        });
    }

    public void updateConnectedBrokers() {
        // Retrieve active brokers from the Directory Service
        Set<String> activeBrokers = directoryServiceClient.getActiveBrokers();
    
        // For each broker in the active broker list
        for (String brokerAddress : activeBrokers) {
            // If we are not already connected to this broker, establish a connection
            if (!connectedBrokerAddresses.contains(brokerAddress)) {
                connectToBroker(brokerAddress); // This method will handle the connection
            }
        }
    }
}
