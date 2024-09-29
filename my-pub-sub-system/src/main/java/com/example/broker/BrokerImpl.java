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
    private final ConcurrentHashMap<String, String> topicNames; // Store both topicId and topicName
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<String>> remoteSubscribers; // Track remote subscribers
    private ServerSocket serverSocket;
    private ExecutorService brokerConnectionPool = Executors.newCachedThreadPool();
    private final CopyOnWriteArrayList<Socket> connectedBrokers = new CopyOnWriteArrayList<>();

    public BrokerImpl(int port) throws IOException {
        this.topics = new ConcurrentHashMap<>();
        this.topicNames = new ConcurrentHashMap<>();
        this.remoteSubscribers = new ConcurrentHashMap<>();
        this.serverSocket = new ServerSocket(port);
        System.out.println("Broker started on port: " + port);
    }

    @Override
    public void start() {
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected: " + clientSocket.getInetAddress());
                new ClientHandler(clientSocket, this).start();  // Handle client connection
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void connectToOtherBroker(String brokerIP, int brokerPort) {
        brokerConnectionPool.submit(() -> {
            try {
                Socket brokerSocket = new Socket(brokerIP, brokerPort);
                connectedBrokers.add(brokerSocket);
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);  // Using 'out'
                System.out.println("Connected to Broker at: " + brokerIP + ":" + brokerPort);
                out.println("Connected to another broker.");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void createTopic(String topicId, String topicName) {
        if (!topicNames.containsKey(topicId)) {
            topics.putIfAbsent(topicId, new CopyOnWriteArrayList<>());
            topicNames.putIfAbsent(topicId, topicName);
            System.out.println("Topic created: " + topicName + " (ID: " + topicId + ")");
    
            // Synchronize topic creation with other brokers
            synchronizeTopic(topicId, topicName);
        } else {
            System.out.println("Topic already exists: " + topicNames.get(topicId));
        }
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
            }

            // Deliver message to remote subscribers (from other brokers)
            if (remoteSubscribers.containsKey(topicId)) {
                for (String remoteSubId : remoteSubscribers.get(topicId)) {
                    System.out.println("Delivering message to remote subscriber: " + remoteSubId);
                }
            }

            if (subscribers == null || subscribers.isEmpty()) {
                System.out.println("No subscribers for topic: " + topicNames.get(topicId));
            }
        } else {
            System.out.println("Topic not found: " + topicId);
        }

        // Synchronize message with other brokers
        synchronizeMessage(topicId, message);
    }

    @Override
    public void addSubscriber(String topicId, Subscriber subscriber) {
        if (!topics.containsKey(topicId)) {
            System.out.println("Topic does not exist locally, requesting topic metadata");
            requestTopicFromOtherBrokers(topicId);
        }

        CopyOnWriteArrayList<Subscriber> subscribers = topics.computeIfAbsent(topicId, k -> new CopyOnWriteArrayList<>());

        if (subscribers.contains(subscriber)) {
            System.out.println("Subscriber " + subscriber.getSubscriberId() + " is already subscribed to topic: " + topicNames.get(topicId));
        } else {
            subscribers.add(subscriber);
            System.out.println("Subscriber " + subscriber.getSubscriberId() + " added to topic: " + topicNames.get(topicId));

            // Synchronize subscription with other brokers
            synchronizeSubscription(topicId, subscriber.getSubscriberId());
        }
    }

    public void addRemoteSubscriber(String topicId, String subscriberId) {
        remoteSubscribers.computeIfAbsent(topicId, k -> new CopyOnWriteArrayList<>()).add(subscriberId);
        System.out.println("Remote subscriber " + subscriberId + " added to topic: " + topicNames.get(topicId));
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

    // Synchronize topic creation across brokers
    @Override
    public void synchronizeTopic(String topicId, String topicName) {
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("synchronize_topic " + topicId + " " + topicName);
                System.out.println("Synchronizing topic " + topicName + " (ID: " + topicId + ") with other brokers");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Request topic metadata from other brokers
    public void requestTopicFromOtherBrokers(String topicId) {
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

    // Synchronize subscription across brokers
    @Override
    public void synchronizeSubscription(String topicId, String subscriberId) {
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("synchronize_sub " + topicId + " " + subscriberId);
                System.out.println("Synchronizing subscription of " + subscriberId + " for topic " + topicId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Synchronize message propagation across brokers
    public void synchronizeMessage(String topicId, String message) {
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("synchronize_message " + topicId + " " + message);
                System.out.println("Synchronizing message to connected brokers: " + message);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
