package com.example.publisher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class PublisherImpl implements Publisher {
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    public PublisherImpl(String brokerHost, int brokerPort) throws IOException {
        this.socket = new Socket(brokerHost, brokerPort);  // Connect to the broker
        this.out = new PrintWriter(socket.getOutputStream(), true);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));  // For receiving responses from the broker
        System.out.println("Connected to broker at " + brokerHost + ":" + brokerPort);
    }

    @Override
    public void createTopic(String topicId, String topicName) {
        out.println("create " + topicId + " " + topicName);  // Send topic ID and topic name
        receiveBrokerResponse();
    }

    @Override
    public void publishMessage(String topicId, String message) {
        out.println("publish " + topicId + " " + message);
        receiveBrokerResponse();
    }

    @Override
    public void showSubscriberCount(String topicId) {
        out.println("show " + topicId);
        receiveBrokerResponse();
    }

    @Override
    public void deleteTopic(String topicId) {
        out.println("delete " + topicId);
        receiveBrokerResponse();
    }

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

    public void closeConnection() {
        try {
            if (out != null) out.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
