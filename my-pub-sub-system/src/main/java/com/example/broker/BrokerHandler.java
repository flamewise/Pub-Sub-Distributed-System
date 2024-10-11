/**
 * Name: Simon Chen
 * Surname: Chen
 * Student ID: 1196439
 *
 * Description: The BrokerHandler class manages communication between brokers. It is responsible for handling
 * inter-broker messaging, including topic synchronization and subscription updates across multiple brokers
 * in the network.
 * 
 * Date: 11/10/2024
 */
package com.example.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import com.example.subscriber.Subscriber;

public class BrokerHandler extends Thread {
    private final Socket brokerSocket;
    private final Broker broker;
    private final String brokerAddress;
    private PrintWriter out;

    public BrokerHandler(Socket brokerSocket, Broker broker, String brokerAddress) {
        this.brokerSocket = brokerSocket;
        this.broker = broker;
        this.brokerAddress = brokerAddress;
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(brokerSocket.getInputStream()));
             PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true)) {

            this.out = out;
            handleBrokerCommands(in);

        } catch (IOException e) {
            System.err.println("Broker disconnected abruptly: " + brokerSocket.getInetAddress());
        } finally {
            closeBrokerSocket();
        }
    }

    private void handleBrokerCommands(BufferedReader in) throws IOException {
        // Print the IP address and port of the broker
        String brokerIP = brokerSocket.getInetAddress().getHostAddress();
        int brokerPort = brokerSocket.getPort();
        System.out.println("Handling commands from broker at IP: " + brokerIP + " Port: " + brokerPort);
        System.out.println("Handling commands from broker at IP: " + brokerIP + " Port: " + brokerPort + 
                   ". Full socket info: Local Address: " + brokerSocket.getLocalAddress() + 
                   " Local Port: " + brokerSocket.getLocalPort() + 
                   " Remote Address: " + brokerSocket.getRemoteSocketAddress());


        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            System.out.println("Broker Command: " + inputLine + " from broker at IP: " + brokerIP + " Port: " + brokerPort);
            String[] parts = inputLine.split(" ");

            if (parts.length > 0) {
                String command = parts[0];
                handleCommand(command, parts);
            } else {
                out.println("Invalid broker command.");
            }
        }
    }
    
    private void handleCommand(String command, String[] parts) {
        try {
            switch (command) {
                case "synchronize_topic":
                    handleSynchronizeTopic(parts);
                    break;
                case "synchronize_message":
                    handleSynchronizeMessage(parts);
                    break;
                case "synchronize_sub":
                    handleSynchronizeSubscription(parts);
                    break;
                case "synchronize_unsub":
                    handleSynchronizeUnsubscribe(parts);
                    break;
                case "synchronize_delete":
                    handleSynchronizeDelete(parts);
                    break;
                case "request_lock":
                    handleLockRequest();
                    break;
                case "release_lock":
                    handleLockRelease();
                    break;
                case "get_local_subscriber_count":
                    handleGetLocalSubscriberCount();
                    break;
                default:
                    System.out.println("Invalid command for broker, Command: " + command);
            }
        } catch (Exception e) {
            out.println("Error processing broker command: " + e.getMessage());
        }
    }

    private void handleGetLocalSubscriberCount() {
        int localSubscriberCount = broker.getLocalSubscriberCount();
        out.println(localSubscriberCount);  // Send the local subscriber count back to the requesting broker
        out.flush();  // Ensure the message is sent
    }
    

    private void handleLockRequest() {
        // Lock this broker
        broker.lock();
    
        // Send lock acknowledgment
        out.println("lock_ack");
        out.flush();
    }

    private void handleLockRelease() {
        // Unlock this broker
        broker.unlock();
    }
    
    private void handleSynchronizeDelete(String[] parts) {
        if (parts.length == 2) {
            String topicId = parts[1];
    
            // Call the existing deleteTopic method in the broker
            broker.deleteTopic(topicId, false); // false to avoid re-synchronizing the deletion
    
            System.out.println("Synchronized deletion of topic: " + topicId);
        } else {
            out.println("Invalid synchronize_delete message.");
        }
    }

    private void handleSynchronizeTopic(String[] parts) {
        if (parts.length == 4) {  // Now expecting 4 parts (synchronize_topic <topicId> <topicName> <username>)
            String topicId = parts[1];
            String topicName = parts[2];
            String username = parts[3];  // The username who created the topic
            broker.createSimpleTopic(username, topicId, topicName); // Only create topic, no further synchronized topic call to prevent recursion
        } else {
            out.println("Invalid synchronize_topic message.");
        }
    }
    
    private void handleSynchronizeUnsubscribe(String[] parts) {
        if (parts.length == 3) {
            String topicId = parts[1];
            String subscriberId = parts[2];
    
            // Unsubscribe the subscriber from the topic with synchronization disabled to prevent recursion
            broker.unsubscribe(topicId, subscriberId, false);
    
            // Optionally log the action or perform any necessary steps here
            System.out.println("Synchronized unsubscription for subscriber: " + subscriberId + " from topic: " + topicId);
        } else {
            out.println("Invalid synchronize_unsub message.");
        }
    }
    

    private void handleSynchronizeMessage(String[] parts) {
        if (parts.length >= 3) {
            String topicId = parts[1];
            // Combine all elements from parts[2] onward into a single message string
            StringBuilder messageBuilder = new StringBuilder(parts[2]);
            for (int i = 3; i < parts.length; i++) {
                messageBuilder.append(" ").append(parts[i]);
            }
            String message = messageBuilder.toString();
            // Publish the message to the topic without synchronization
            broker.publishMessage(topicId, message, false);
        } else {
            out.println("Invalid synchronize_message command.");
        }
    }
    

    private void handleSynchronizeSubscription(String[] parts) {
        if (parts.length == 3) {
            String topicId = parts[1];
            String subscriberId = parts[2];
    
            // Add the subscriber ID to the broker's subscription list without creating a Subscriber object
            broker.addSubscriberId(topicId, subscriberId, false);
            
            // Optionally, log the action or perform any necessary steps here
            System.out.println("Synchronized subscription for subscriber: " + subscriberId + " to topic: " + topicId);
        } else {
            out.println("Invalid synchronize_sub message.");
        }
    }
    

    private void closeBrokerSocket() {
        try {
            if (!brokerSocket.isClosed()) {
                brokerSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Socket getSocket() {
        return this.brokerSocket;
    }

}
