package com.example.broker;

import com.example.subscriber.Subscriber;
import java.io.PrintWriter;

public interface Broker {
    void createTopic(String topicName);
    void publishMessage(String topicName, String message);
    void addSubscriber(String topicName, Subscriber subscriber);
    void removeSubscriber(String topicName, Subscriber subscriber);
    void unsubscribe(String topicName, String subscriberId);
    
    // Start the broker (e.g., listen for connections)
    void start();
    
    // Return subscriber count for a given topic
    int getSubscriberCount(String topicId);

    // Remove a topic
    void removeTopic(String topicId);

    // List all topics available
    void listAllTopics(PrintWriter out);

    // List current subscriptions of a subscriber
    void listSubscriptions(PrintWriter out, String subscriberId);
}
