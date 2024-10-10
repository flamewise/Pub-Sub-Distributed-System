package com.example.subscriber;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

public class Subscriber {
    private final String username;
    private final PrintWriter out;
    private final BufferedReader in;

    public Subscriber(String username, PrintWriter out, BufferedReader in) {
        this.username = username;
        this.out = out;
        this.in = in;
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

    // Method to list all available topics
    public void listAllTopics() {
        out.println("list_all");  // Send request to broker to list all available topics

        // Receive and print the list of topics from the broker
        try {
            String response;
            while ((response = in.readLine()) != null) {
                if ("END".equals(response)) {
                    break;  // End of topic list
                }
                System.out.println(response);  // Print each topic
            }
        } catch (IOException e) {
            System.err.println("Error reading topic list: " + e.getMessage());
        }
    }

    // Method to show current subscriptions
    public void showCurrentSubscriptions() {
        out.println("current");  // Send request to broker to list current subscriptions

        // Receive and print the list of current subscriptions from the broker
        try {
            String response;
            while ((response = in.readLine()) != null) {
                if ("END".equals(response)) {
                    break;  // End of subscription list
                }
                System.out.println(response);  // Print each subscription
            }
        } catch (IOException e) {
            System.err.println("Error reading current subscriptions: " + e.getMessage());
        }
    }

}
