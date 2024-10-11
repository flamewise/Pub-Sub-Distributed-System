/**
 * Name: Simon Chen
 * Surname: Chen
 * Student ID: 1196439
 *
 * Description: The Publisher class represents a publisher in the publisher-subscriber system. Publishers
 * create topics and publish messages to them, which are distributed to all subscribed clients. The class
 * manages interaction between the publisher and the broker.
 * 
 * Date: 11/10/2024
 */
package com.example.publisher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

public class Publisher {
    private final PrintWriter out;
    private final BufferedReader in;

    // Constructor to use existing PrintWriter and BufferedReader
    public Publisher(PrintWriter out, BufferedReader in) {
        this.out = out;
        this.in = in;
    }

    // Method to create a new topic
    public void createTopic(String topicId, String topicName) {
        out.println("create " + topicId + " " + topicName);  // Send topic ID and topic name
        receiveBrokerResponse();
    }

    // Method to publish a message to a topic, limited to 100 characters
    public void publishMessage(String topicId, String message) {
        if (message.length() > 100) {
            System.out.println("Error: Message exceeds 100 characters. Please shorten your message.");
            return;
        }
        
        out.println("publish " + topicId + " " + message);  // Send topic ID and message
        receiveBrokerResponse();
    }

    // Method to show subscriber count for a topic
    public void showSubscriberCount(String topicId) {
        out.println("show " + topicId);  // Send topic ID
        receiveBrokerResponse();
    }

    // Method to delete a topic
    public void deleteTopic(String topicId) {
        out.println("delete " + topicId);  // Send topic ID
        receiveBrokerResponse();
    }

    // Method to receive a response from the broker
    private void receiveBrokerResponse() {
        try {
            String response;
            while ((response = in.readLine()) != null) {
                System.out.println(response);
                break;  // Read only one response from the broker
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
