package com.example.subscriber;

public interface Subscriber {
    void subscribe(String topicName);
    void receiveMessage(String topicName, String message);
    void unsubscribe(String topicName);

    // Renamed to avoid conflict with Thread's getId()
    String getSubscriberId();
}
