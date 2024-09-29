package com.example.broker;

import java.io.PrintWriter;

public interface Broker {
    void start();
    void connectToOtherBroker(String brokerIP, int brokerPort);
    
    void createTopic(String topicId, String topicName);
    
    void publishMessage(String topicId, String message);
    void addSubscriber(String topicId, com.example.subscriber.Subscriber subscriber);
    void removeSubscriber(String topicId, com.example.subscriber.Subscriber subscriber);
    void unsubscribe(String topicId, String subscriberId);
    int getSubscriberCount(String topicId);
    void removeTopic(String topicId);
    void listAllTopics(PrintWriter out);
    void listSubscriptions(PrintWriter out, String subscriberId);

    // New methods for synchronization across brokers
    void synchronizeTopic(String topicId, String topicName);
    void synchronizeSubscription(String topicId, String subscriberId);
    void synchronizeMessage(String topicId, String message);


    void requestTopicFromOtherBrokers(String topicId);  // Added this method here
}
