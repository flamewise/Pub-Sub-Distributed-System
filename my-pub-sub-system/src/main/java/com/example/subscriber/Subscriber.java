package com.example.subscriber;

import java.io.PrintWriter;

public class Subscriber {
    private final String username;
    private final PrintWriter out;

    public Subscriber(String username, PrintWriter out) {
        this.username = username;
        this.out = out;
    }

    // Method to receive a message on a topic
    public void receiveMessage(String topicName, String message) {
        out.println("Received message on topic " + topicName + ": " + message);
    }

    // Get the username
    public String getUsername() {
        return username;
    }

    // Subscribe to a topic
    public void subscribe(String topicName) {
        out.println("sub " + topicName);  // Send subscribe command and topic name
    }

    // Unsubscribe from a topic
    public void unsubscribe(String topicName) {
        out.println("unsub " + topicName);  // Send unsubscribe command and topic name
    }
}
