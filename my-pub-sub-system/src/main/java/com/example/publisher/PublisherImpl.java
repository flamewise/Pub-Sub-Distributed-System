package com.example.publisher;

import com.example.broker.Broker;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

public class PublisherImpl implements Publisher {
    private Socket socket;
    private PrintWriter out;

    public PublisherImpl(String brokerHost, int brokerPort) throws IOException {
        this.socket = new Socket(brokerHost, brokerPort);  // Connect to the broker
        this.out = new PrintWriter(socket.getOutputStream(), true);
        System.out.println("Connected to broker at " + brokerHost + ":" + brokerPort);
    }

    @Override
    public void createTopic(String topicName) {
        out.println("create " + topicName);
    }

    @Override
    public void publishMessage(String topicName, String message) {
        out.println("publish " + topicName + " " + message);
    }

    public void closeConnection() {
        try {
            if (out != null) out.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
}
