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
    private final String subscriberId;

    // Constructor that establishes a connection to the broker
    public SubscriberImpl(String host, int port, String subscriberId) throws IOException {
        this.socket = new Socket(host, port);  // Connect to the broker
        this.out = new PrintWriter(socket.getOutputStream(), true);  // Output stream to send commands
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));  // Input stream to receive responses
        this.subscriberId = subscriberId;

        System.out.println("Connected to broker at " + host + ":" + port);

        // Start a thread to listen for incoming messages from the broker
        new Thread(() -> {
            try {
                String messageFromBroker;
                while ((messageFromBroker = in.readLine()) != null) {
                    System.out.println("Broker: " + messageFromBroker);  // Display message received from the broker
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
        // This method is not directly used by the subscriber in this case
        System.out.println("Received message on topic " + topicName + ": " + message);
    }

    @Override
    public void unsubscribe(String topicName) {
        out.println("unsub " + topicName);  // Send the unsubscription command to the broker
        System.out.println("Unsubscribed from topic: " + topicName);
    }

    @Override
    public String getSubscriberId() {
        return subscriberId;
    }
}
