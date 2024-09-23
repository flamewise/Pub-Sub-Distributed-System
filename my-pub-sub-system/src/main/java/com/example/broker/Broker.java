package com.example.broker;

import java.io.PrintWriter;

public interface Broker {
    void start();
    void connectToOtherBroker(String brokerIP, int brokerPort);
    
    // Updated createTopic method to accept both topicId and topicName
    void createTopic(String topicId, String topicName);
    
    void publishMessage(String topicId, String message);
    void addSubscriber(String topicId, com.example.subscriber.Subscriber subscriber);
    void removeSubscriber(String topicId, com.example.subscriber.Subscriber subscriber);
    void unsubscribe(String topicId, String subscriberId);
    int getSubscriberCount(String topicId);
    void removeTopic(String topicId);
    void listAllTopics(PrintWriter out);
    void listSubscriptions(PrintWriter out, String subscriberId);
}
