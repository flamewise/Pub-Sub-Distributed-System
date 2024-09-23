package com.example.publisher;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

public class PublisherImpl implements Publisher {
    private Socket socket;
    private PrintWriter out;

    public PublisherImpl(String brokerIp, int brokerPort) throws IOException {
        this.socket = new Socket(brokerIp, brokerPort);
        this.out = new PrintWriter(socket.getOutputStream(), true);
        System.out.println("Connected to broker at " + brokerIp + ":" + brokerPort);
    }

    @Override
    public void createTopic(String topicName) {
        out.println("create " + topicName);
    }

    @Override
    public void publishMessage(String topicName, String message) {
        out.println("publish " + topicName + " " + message);
    }
}
