package com.example.broker;

import com.example.subscriber.Subscriber;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class BrokerImpl implements Broker {
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Subscriber>> topics;
    private ServerSocket serverSocket;

    // Constructor that takes a port number
    public BrokerImpl(int port) throws IOException {
        this.topics = new ConcurrentHashMap<>();
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
    public void createTopic(String topicName) {
        topics.putIfAbsent(topicName, new CopyOnWriteArrayList<>());
    }

    @Override
    public void publishMessage(String topicName, String message) {
        CopyOnWriteArrayList<Subscriber> subscribers = topics.get(topicName);
        if (subscribers != null) {
            for (Subscriber subscriber : subscribers) {
                subscriber.receiveMessage(topicName, message);
            }
        }
    }

    @Override
    public void addSubscriber(String topicName, Subscriber subscriber) {
        topics.computeIfPresent(topicName, (key, subscribers) -> {
            subscribers.add(subscriber);
            return subscribers;
        });
    }

    @Override
    public void removeSubscriber(String topicName, Subscriber subscriber) {
        CopyOnWriteArrayList<Subscriber> subscribers = topics.get(topicName);
        if (subscribers != null) {
            subscribers.remove(subscriber);
        }
    }

    @Override
    public void unsubscribe(String topicName, String subscriberId) {
        CopyOnWriteArrayList<Subscriber> subscribers = topics.get(topicName);
        if (subscribers != null) {
            subscribers.removeIf(sub -> sub.getSubscriberId().equals(subscriberId));
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
    }

    @Override
    public void listAllTopics(PrintWriter out) {
        for (String topicId : topics.keySet()) {
            out.println("Topic: " + topicId);
        }
    }

    @Override
    public void listSubscriptions(PrintWriter out, String subscriberId) {
        for (String topicId : topics.keySet()) {
            CopyOnWriteArrayList<Subscriber> subscribers = topics.get(topicId);
            for (Subscriber subscriber : subscribers) {
                if (subscriber.getSubscriberId().equals(subscriberId)) {
                    out.println("Subscribed to: " + topicId);
                }
            }
        }
    }
}
