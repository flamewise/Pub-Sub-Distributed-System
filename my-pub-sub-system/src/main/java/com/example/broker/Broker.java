package com.example.broker;

import com.example.subscriber.Subscriber;

import java.io.PrintWriter;
import java.util.Set;

public interface Broker {

    // Start the broker service and listen for connections
    void start();

    // Establish connection with another broker in the network
    void connectToBroker(String brokerIP, int brokerPort);

    // Topic management
    void createTopic(String topicId, String topicName);
    void removeTopic(String topicId);
    void listAllTopics(PrintWriter out);

    // Message publishing
    void publishMessage(String topicId, String message);
    void publishMessageToLocalSubscribers(String topicId, String message);  // Send message to local subscribers

    // Subscriber management
    void addSubscriber(String topicId, Subscriber subscriber);
    void removeSubscriber(String topicId, Subscriber subscriber);
    void unsubscribe(String topicId, String subscriberId);
    int getSubscriberCount(String topicId);
    void listSubscriptions(PrintWriter out, String subscriberId);

    // Synchronization between brokers
    void synchronizeTopic(String topicId, String topicName);
    void synchronizeMessage(String topicId, String message);
    void synchronizeSubscription(String topicId, String subscriberId);

    // Request metadata from other brokers
    void requestTopicFromBrokers(String topicId);

    // New method to get connected brokers' addresses
    Set<String> getConnectedBrokerAddresses();
}