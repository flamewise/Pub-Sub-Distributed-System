/**
 * Name: Simon Chen
 * Surname: Chen
 * Student ID: 1196439
 *
 * Description: The Subscriber class represents a subscriber in the publisher-subscriber system. Subscribers
 * can subscribe to topics and receive messages published by publishers. The class stores the subscriber's
 * identity and manages their interaction with the broker.
 * 
 * Date: 11/10/2024
 */
package com.example.subscriber;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Subscriber {
    private final String username;
    private final PrintWriter out;
    private final BufferedReader in;
    private final BlockingQueue<String> commandResponseQueue = new LinkedBlockingQueue<>(); // Queue for command responses

    public Subscriber(String username, PrintWriter out, BufferedReader in) {
        this.username = username;
        this.out = out;
        this.in = in;
    }

    // Method to receive a message on a topic and display it in the correct format
    public void receiveMessage(String topicId, String topicName, String message) {
        // Get current time in the format dd/MM HH:mm:ss
        String timestamp = new SimpleDateFormat("dd/MM HH:mm:ss").format(new Date());

        // Format the message and print it to the subscriber's console
        System.out.println(timestamp + " " + topicId + ":" + topicName + ": " + message);
    }

    // Method to handle async messages
    public void handleAsyncMessage(String message) {
        System.out.println(message);
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
        out.flush();

        try {
            String response;
            // Block and read from the command response queue
            while (!(response = commandResponseQueue.take()).equals("END")) {
                System.out.println(response);  // Print each topic
            }
        } catch (InterruptedException e) {
            System.err.println("Error reading topic list: " + e.getMessage());
        }
    }

    // Method to show current subscriptions
    public void showCurrentSubscriptions() {
        out.println("current");
        out.flush();

        try {
            String response;
            // Block and read from the command response queue
            while (!(response = commandResponseQueue.take()).equals("END")) {
                System.out.println(response);  // Print each subscription
                System.out.println("Wait for response");
            }
        } catch (InterruptedException e) {
            System.err.println("Error reading current subscriptions: " + e.getMessage());
        }
    }

    // Method to add command response to the queue
    public void addCommandResponse(String response) {
        try {
            commandResponseQueue.put(response);
        } catch (InterruptedException e) {
            System.err.println("Error adding response to queue: " + e.getMessage());
        }
    }
}
