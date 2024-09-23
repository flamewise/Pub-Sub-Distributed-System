package com.example.broker;

import com.example.subscriber.Subscriber;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BrokerImpl implements Broker {
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Subscriber>> topics;
    private final ConcurrentHashMap<String, String> topicNames; // New Map to store topicId and topicName
    private ServerSocket serverSocket;
    private ExecutorService brokerConnectionPool = Executors.newCachedThreadPool(); // Pool for handling broker connections

    public BrokerImpl(int port) throws IOException {
        this.topics = new ConcurrentHashMap<>();
        this.topicNames = new ConcurrentHashMap<>(); // Initialize map for topic names
        this.serverSocket = new ServerSocket(port);  // Initialize the ServerSocket with the port number
        System.out.println("Broker started on port: " + port);
    }

    @Override
    public void start() {
        while (true) {
            try {
                // Accept incoming connections and spawn ClientHandler threads
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected: " + clientSocket.getInetAddress());
                new ClientHandler(clientSocket, this).start();  // Create a new thread to handle the client
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void connectToOtherBroker(String brokerIP, int brokerPort) {
        brokerConnectionPool.submit(() -> {
            try {
                Socket brokerSocket = new Socket(brokerIP, brokerPort);  // Establish connection with other broker
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                System.out.println("Connected to Broker at: " + brokerIP + ":" + brokerPort);
                // You can add additional logic here to handle communication with the other broker
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    // Updated createTopic method to handle both topicId and topicName
    @Override
    public void createTopic(String topicId, String topicName) {
        topics.putIfAbsent(topicId, new CopyOnWriteArrayList<>());
        topicNames.putIfAbsent(topicId, topicName);  // Store topicName with its ID
        System.out.println("Topic created: " + topicName + " (ID: " + topicId + ")");
    }

    @Override
    public void publishMessage(String topicId, String message) {
        if (topics.containsKey(topicId)) {
            CopyOnWriteArrayList<Subscriber> subscribers = topics.get(topicId);
            if (subscribers != null && !subscribers.isEmpty()) {
                for (Subscriber subscriber : subscribers) {
                    System.out.println("Delivering message to subscriber: " + subscriber.getSubscriberId());
                    subscriber.receiveMessage(topicId, message);  // Send the message to each subscriber
                }
            } else {
                System.out.println("No subscribers for topic: " + topicNames.get(topicId)); // Use topicName
            }
        } else {
            System.out.println("Topic not found: " + topicId);
        }
    }

    @Override
    public void addSubscriber(String topicId, Subscriber subscriber) {
        topics.computeIfAbsent(topicId, k -> new CopyOnWriteArrayList<>()).add(subscriber);
        System.out.println("Subscriber " + subscriber.getSubscriberId() + " added to topic: " + topicNames.get(topicId));
    }

    @Override
    public void removeSubscriber(String topicId, Subscriber subscriber) {
        CopyOnWriteArrayList<Subscriber> subscribers = topics.get(topicId);
        if (subscribers != null) {
            subscribers.remove(subscriber);
            System.out.println("Subscriber " + subscriber.getSubscriberId() + " removed from topic: " + topicNames.get(topicId));
        }
    }

    @Override
    public void unsubscribe(String topicId, String subscriberId) {
        CopyOnWriteArrayList<Subscriber> subscribers = topics.get(topicId);
        if (subscribers != null) {
            subscribers.removeIf(sub -> sub.getSubscriberId().equals(subscriberId));
            System.out.println("Subscriber with ID " + subscriberId + " unsubscribed from topic: " + topicNames.get(topicId));
        }
    }

    @Override
    public int getSubscriberCount(String topicId) {
        CopyOnWriteArrayList<Subscriber> subscribers = topics.get(topicId);
        return (subscribers != null) ? subscribers.size() : 0;
    }

    @Override
    public void removeTopic(String topicId) {
        topics.remove(topicId);
        topicNames.remove(topicId);
        System.out.println("Topic removed: " + topicId);
    }

    @Override
    public void listAllTopics(PrintWriter out) {
        for (String topicId : topics.keySet()) {
            out.println("Topic: " + topicId + " (" + topicNames.get(topicId) + ")");
        }
    }

    @Override
    public void listSubscriptions(PrintWriter out, String subscriberId) {
        for (String topicId : topics.keySet()) {
            CopyOnWriteArrayList<Subscriber> subscribers = topics.get(topicId);
            for (Subscriber subscriber : subscribers) {
                if (subscriber.getSubscriberId().equals(subscriberId)) {
                    out.println("Subscribed to: " + topicId + " (" + topicNames.get(topicId) + ")");
                }
            }
        }
    }
}
