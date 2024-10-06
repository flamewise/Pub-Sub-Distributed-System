package com.example.broker;

import com.example.subscriber.Subscriber;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BrokerImpl implements Broker {
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Subscriber>> topicSubscribers;
    private final ConcurrentHashMap<String, String> topicNames;
    private ServerSocket serverSocket;
    private final ExecutorService connectionPool;
    private final CopyOnWriteArrayList<Socket> connectedBrokers;
    private final Set<String> connectedBrokerAddresses = ConcurrentHashMap.newKeySet(); // Stores connected brokers

    public BrokerImpl(int port) throws IOException {
        this.topicSubscribers = new ConcurrentHashMap<>();
        this.topicNames = new ConcurrentHashMap<>();
        this.serverSocket = new ServerSocket(port);
        this.connectionPool = Executors.newCachedThreadPool();
        this.connectedBrokers = new CopyOnWriteArrayList<>();
        System.out.println("Broker started on port: " + port);
    }

    @Override
    public void start() {
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected: " + clientSocket.getInetAddress());
                connectionPool.submit(new ClientHandler(clientSocket, this));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public synchronized void createTopic(String topicId, String topicName) {
        if (!topicNames.containsKey(topicId)) {
            topicSubscribers.putIfAbsent(topicId, new CopyOnWriteArrayList<>());
            topicNames.put(topicId, topicName);
            System.out.println("Topic created: " + topicName + " (ID: " + topicId + ")");

            // Synchronize topic creation with other brokers
            synchronizeTopic(topicId, topicName);
        } else {
            System.out.println("Topic already exists: " + topicNames.get(topicId));
        }
    }

    @Override
    public synchronized void publishMessage(String topicId, String message) {
        if (topicSubscribers.containsKey(topicId)) {
            CopyOnWriteArrayList<Subscriber> subscribers = topicSubscribers.get(topicId);

            // Deliver the message to local subscribers
            if (subscribers != null && !subscribers.isEmpty()) {
                for (Subscriber subscriber : subscribers) {
                    System.out.println("Delivering message to local subscriber: " + subscriber.getSubscriberId());
                    subscriber.receiveMessage(topicId, message);
                }
            }

            // Now forward the message to other brokers for their local subscribers
            synchronizeMessage(topicId, message);
        } else {
            System.out.println("Topic not found: " + topicId);
        }
    }

    @Override
    public synchronized void publishMessageToLocalSubscribers(String topicId, String message) {
        if (topicSubscribers.containsKey(topicId)) {
            CopyOnWriteArrayList<Subscriber> subscribers = topicSubscribers.get(topicId);

            // Deliver message to local subscribers only
            if (subscribers != null && !subscribers.isEmpty()) {
                for (Subscriber subscriber : subscribers) {
                    System.out.println("Delivering message to local subscriber: " + subscriber.getSubscriberId());
                    subscriber.receiveMessage(topicId, message);
                }
            } else {
                System.out.println("No local subscribers for topic: " + topicId);
            }
        } else {
            System.out.println("Topic not found: " + topicId);
        }
    }

    @Override
    public synchronized void synchronizeMessage(String topicId, String message) {
        if (connectedBrokers.isEmpty()) {
            System.out.println("No brokers connected to forward the message.");
            return;
        }

        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("synchronize_message " + topicId + " " + message);

                // Print the socket information (IP and port)
                String remoteAddress = brokerSocket.getInetAddress().getHostAddress();
                int remotePort = brokerSocket.getPort();
                int localPort = brokerSocket.getLocalPort();

                System.out.println("Forwarding message to broker at " + remoteAddress + ":" + remotePort +
                                   " from local port " + localPort + " for topic: " + topicId);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void connectToBroker(String brokerIP, int brokerPort) {
        String brokerAddress = brokerIP + ":" + brokerPort;

        // Check if the broker is already connected
        if (connectedBrokerAddresses.contains(brokerAddress)) {
            System.out.println("Already connected to broker at: " + brokerAddress);
            return;  // Avoid reconnecting
        }

        connectionPool.submit(() -> {
            try {
                Socket brokerSocket = new Socket(brokerIP, brokerPort);
                connectedBrokers.add(brokerSocket);
                connectedBrokerAddresses.add(brokerAddress);  // Mark as connected
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                System.out.println("Connected to Broker at: " + brokerIP + ":" + brokerPort);

                // Send a message back to the existing broker to let them know this broker has connected
                out.println("broker_connect " + serverSocket.getInetAddress().getHostAddress() + " " + serverSocket.getLocalPort());
                System.out.println("Sent broker_connect message to existing broker at: " + brokerIP + ":" + brokerPort);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public Set<String> getConnectedBrokerAddresses() {
        return connectedBrokerAddresses;
    }

    @Override
    public synchronized void synchronizeTopic(String topicId, String topicName) {
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("synchronize_topic " + topicId + " " + topicName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public synchronized void synchronizeSubscription(String topicId, String subscriberId) {
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("synchronize_sub " + topicId + " " + subscriberId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public synchronized void addSubscriber(String topicId, Subscriber subscriber) {
        CopyOnWriteArrayList<Subscriber> subscribers = topicSubscribers.computeIfAbsent(topicId, k -> new CopyOnWriteArrayList<>());
        if (!subscribers.contains(subscriber)) {
            subscribers.add(subscriber);
            System.out.println("Subscriber added to topic: " + topicId);
            synchronizeSubscription(topicId, subscriber.getSubscriberId());
        }
    }

    @Override
    public synchronized int getSubscriberCount(String topicId) {
        CopyOnWriteArrayList<Subscriber> subscribers = topicSubscribers.get(topicId);
        return (subscribers != null) ? subscribers.size() : 0;
    }

    @Override
    public synchronized void unsubscribe(String topicId, String subscriberId) {
        CopyOnWriteArrayList<Subscriber> subscribers = topicSubscribers.get(topicId);
        if (subscribers != null) {
            subscribers.removeIf(subscriber -> subscriber.getSubscriberId().equals(subscriberId));
            System.out.println("Subscriber with ID " + subscriberId + " unsubscribed from topic: " + topicId);
        }
    }

    @Override
    public synchronized void removeSubscriber(String topicId, Subscriber subscriber) {
        CopyOnWriteArrayList<Subscriber> subscribers = topicSubscribers.get(topicId);
        if (subscribers != null) {
            subscribers.remove(subscriber);
            System.out.println("Subscriber removed from topic: " + topicId);
        }
    }

    @Override
    public synchronized void requestTopicFromBrokers(String topicId) {
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("request_topic " + topicId);
                System.out.println("Requesting topic metadata for " + topicId + " from other brokers");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public synchronized void removeTopic(String topicId) {
        topicSubscribers.remove(topicId);
        topicNames.remove(topicId);
        System.out.println("Topic removed: " + topicId);
    }

    @Override
    public synchronized void listSubscriptions(PrintWriter out, String subscriberId) {
        for (String topicId : topicSubscribers.keySet()) {
            CopyOnWriteArrayList<Subscriber> subscribers = topicSubscribers.get(topicId);
            for (Subscriber subscriber : subscribers) {
                if (subscriber.getSubscriberId().equals(subscriberId)) {
                    out.println("Subscribed to: " + topicId + " (" + topicNames.get(topicId) + ")");
                }
            }
        }
    }

    @Override
    public synchronized void listAllTopics(PrintWriter out) {
        for (String topicId : topicSubscribers.keySet()) {
            out.println("Topic: " + topicId + " (" + topicNames.get(topicId) + ")");
        }
    }
}