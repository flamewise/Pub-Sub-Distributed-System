package com.example.publisher;

public interface Publisher {
    void createTopic(String topicId, String topicName);
    void publishMessage(String topicId, String message);
    void showSubscriberCount(String topicId);  // New method
    void deleteTopic(String topicId);  // New method
}
