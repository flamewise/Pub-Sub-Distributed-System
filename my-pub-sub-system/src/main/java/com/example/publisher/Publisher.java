package com.example.publisher;

public interface Publisher {
    void createTopic(String topicName);
    void publishMessage(String topicName, String message);
}
