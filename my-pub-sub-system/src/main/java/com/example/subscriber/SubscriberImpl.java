package com.example.subscriber;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class SubscriberImpl implements Subscriber {
    private final Socket socket;
    private final PrintWriter out;
    private final BufferedReader in;
    private final String id;

    public SubscriberImpl(String host, int port, String id) throws IOException {
        this.socket = new Socket(host, port);  // Connect to the broker
        this.out = new PrintWriter(socket.getOutputStream(), true);  // Set up the output stream for sending commands
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));  // Set up the input stream for receiving messages
        this.id = id;
        System.out.println("Connected to broker at " + host + ":" + port);

        // Start a thread to listen for incoming messages from the broker
        new Thread(() -> {
            try {
                String messageFromBroker;
                while ((messageFromBroker = in.readLine()) != null) {
                    System.out.println(messageFromBroker);  // Display message received from broker
                }
            } catch (IOException e) {
                System.err.println("Connection closed by broker.");
            }
        }).start();
    }

    @Override
    public void subscribe(String topicName) {
        out.println("sub " + topicName);  // Send the subscription command to the broker
        System.out.println("Subscribed to topic: " + topicName);
    }

    @Override
    public void receiveMessage(String topicName, String message) {
        // This method won't be called directly on the subscriber side.
        // Instead, the subscriber receives messages from the broker via the input stream (handled in the thread).
        System.out.println("Received message on topic " + topicName + ": " + message);
    }

    @Override
    public void unsubscribe(String topicName) {
        out.println("unsub " + topicName);  // Send the unsubscription command to the broker
        System.out.println("Unsubscribed from topic: " + topicName);
    }

    @Override
    public String getSubscriberId() {
        return id;
    }
}
