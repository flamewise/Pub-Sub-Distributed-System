package com.example.broker;

import com.example.subscriber.Subscriber;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
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

                String inputLine = in.readLine();  
                String[] parts = inputLine.split(" ", 2);  

                if (parts.length == 2) {
                    String username = parts[0];  
                    String connection_type = parts[1];  

                    System.out.println("Client connected: " + clientSocket.getInetAddress() + " with username: " + username + " with connection type " + connection_type);

                    connectionPool.submit(new ClientHandler(clientSocket, this, username, connection_type));

                    if (connection_type.equals("broker")) {
                        String brokerIP = clientSocket.getInetAddress().getHostAddress();
                        int brokerPort = serverSocket.getLocalPort(); 
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
                Socket brokerSocket = new Socket(brokerIP, brokerPort);
                connectedBrokers.add(brokerSocket);
                connectedBrokerAddresses.add(brokerAddress);
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                System.out.println("Connected to Broker at: " + brokerIP + ":" + brokerPort);

                String localHost = brokerSocket.getLocalAddress().getHostAddress();
                int localPort = serverSocket.getLocalPort();
                out.println(localHost + ":" + localPort + " broker");

                connectionPool.submit(new ClientHandler(brokerSocket, this, localHost + ":" + localPort, "broker"));
            } catch (IOException e) {
                System.out.println("Error connecting to broker at " + brokerIP + ":" + brokerPort + ": " + e.getMessage());
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

    // Method to prevent connecting to itself
    public boolean isSelf(String brokerIP, int brokerPort) {
        String selfAddress = serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort();
        String brokerAddress = brokerIP + ":" + brokerPort;
        return selfAddress.equals(brokerAddress);
    }

    public void connectToOtherBrokers(List<String> brokerAddresses) {
        for (String brokerAddress : brokerAddresses) {
            String[] brokerDetails = brokerAddress.split(":");
            if (brokerDetails.length == 2) {
                String brokerIP = brokerDetails[0];
                int brokerPort = Integer.parseInt(brokerDetails[1]);
                if (!isSelf(brokerIP, brokerPort)) {  // Prevent self-connection
                    connectToBroker(brokerIP, brokerPort);  // Connect to each broker
                }
            }
        }
    }

    public void registerWithDirectoryServiceAndConnect(String directoryServiceAddress) {
        try {
            String[] addressParts = directoryServiceAddress.split(":");
            String dirServiceIP = addressParts[0];
            int dirServicePort = Integer.parseInt(addressParts[1]);
    
            Socket dirServiceSocket = new Socket(dirServiceIP, dirServicePort);
            PrintWriter out = new PrintWriter(dirServiceSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(dirServiceSocket.getInputStream()));
    
            String brokerAddress = serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort();
            out.println("register " + brokerAddress);
            System.out.println("Registered with Directory Service at: " + directoryServiceAddress);
    
            out.println("get_brokers");
            String response;
            List<String> brokerAddresses = new ArrayList<>();
            while ((response = in.readLine()) != null && !"END".equals(response)) {
                if (!response.equals(brokerAddress)) {  // Don't connect to self
                    brokerAddresses.add(response);
                }
            }
    
            connectToOtherBrokers(brokerAddresses);
    
            dirServiceSocket.close();
        } catch (IOException e) {
            System.out.println("Failed to register or connect with the Directory Service.");
            e.printStackTrace();  // Better logging of the error
        }
    }
    
}
