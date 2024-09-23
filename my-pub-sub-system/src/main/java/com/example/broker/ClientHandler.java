package com.example.broker;

import com.example.subscriber.Subscriber;

import java.io.*;
import java.net.Socket;

public class ClientHandler extends Thread implements Subscriber {
    private Socket clientSocket;
    private Broker broker;
    private String subscriberId;

    public ClientHandler(Socket socket, Broker broker) {
        this.clientSocket = socket;
        this.broker = broker;
        this.subscriberId = "client_" + clientSocket.getPort();  // Unique ID for each subscriber
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                System.out.println("Received: " + inputLine);

                String[] parts = inputLine.split(" ");
                String command = parts[0];

                switch (command) {
                    case "create":
                        if (parts.length == 3) {
                            String topicName = parts[2];
                            broker.createTopic(topicName);
                            out.println("Topic created: " + topicName);
                        }
                        break;

                    case "publish":
                        if (parts.length >= 3) {
                            String topicId = parts[1];
                            String message = inputLine.substring(inputLine.indexOf(parts[2]));
                            broker.publishMessage(topicId, message);
                            out.println("Message published to " + topicId);
                        }
                        break;

                    case "show":
                        if (parts.length == 2) {
                            String topicId = parts[1];
                            int count = broker.getSubscriberCount(topicId);
                            out.println("Topic: " + topicId + " has " + count + " subscribers.");
                        }
                        break;

                    case "delete":
                        if (parts.length == 2) {
                            String topicId = parts[1];
                            broker.removeTopic(topicId);
                            out.println("Topic deleted: " + topicId);
                        }
                        break;

                    case "list":
                        if (parts.length == 2 && parts[1].equals("all")) {
                            broker.listAllTopics(out);
                        }
                        break;

                    case "sub":
                        if (parts.length == 2) {
                            String topicId = parts[1];
                            broker.addSubscriber(topicId, this);
                            out.println("Subscribed to: " + topicId);
                        }
                        break;

                    case "current":
                        broker.listSubscriptions(out, subscriberId);
                        break;

                    case "unsub":
                        if (parts.length == 2) {
                            String topicId = parts[1];
                            broker.unsubscribe(topicId, subscriberId);
                            out.println("Unsubscribed from: " + topicId);
                        }
                        break;

                    case "exit":
                        out.println("Closing connection...");
                        clientSocket.close();
                        return;  // Exit the while loop

                    default:
                        out.println("Unknown command. Please try again.");
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Subscriber implementation methods
    @Override
    public void receiveMessage(String topicName, String message) {
        System.out.println("Received message on topic " + topicName + ": " + message);
    }

    // Renamed to avoid conflict with Thread's getId()
    @Override
    public String getSubscriberId() {
        return subscriberId;
    }

    @Override
    public void subscribe(String topicName) {
        // Subscription logic
    }

    @Override
    public void unsubscribe(String topicName) {
        // Unsubscription logic
    }
}
