package com.example.publisher;

import com.example.directory.DirectoryServiceClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class PublisherApp {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java -jar publisher.jar <username> <directory_service_ip> <directory_service_port>");
            return;
        }

        try {
            String username = args[0];
            String directoryServiceIP = args[1];
            String directoryServicePort = args[2];

            // Initialize DirectoryServiceClient to fetch available brokers
            DirectoryServiceClient directoryServiceClient = new DirectoryServiceClient(directoryServiceIP + ":" + directoryServicePort);
            
            // Fetch active brokers, ensuring the list is populated until 'END' signal is received
            List<String> brokerAddresses = new ArrayList<>(directoryServiceClient.getActiveBrokers());

            // If no brokers are returned, print a message and exit
            if (brokerAddresses.isEmpty()) {
                System.out.println("No active brokers available.");
                return;
            }

            // Print the list of available brokers
            System.out.println("Available brokers:");
            for (int i = 0; i < brokerAddresses.size(); i++) {
                System.out.println((i + 1) + ". " + brokerAddresses.get(i));
            }

            // Prompt the user to select a broker
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            System.out.print("Select a broker (enter the number): ");
            String input = reader.readLine();

            int selectedBrokerIndex = Integer.parseInt(input) - 1;
            if (selectedBrokerIndex < 0 || selectedBrokerIndex >= brokerAddresses.size()) {
                System.out.println("Invalid broker selection.");
                return;
            }

            // Extract selected broker's IP and port
            String selectedBroker = brokerAddresses.get(selectedBrokerIndex);
            String[] brokerDetails = selectedBroker.split(":");
            String brokerHost = brokerDetails[0];
            int brokerPort = Integer.parseInt(brokerDetails[1]);

            // Establish a connection to the selected broker
            Socket socket = new Socket(brokerHost, brokerPort);
            PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);

            // Send the initial message with username and connection type (publisher)
            out.println(username + " publisher");

            // Now the Publisher can interact with the broker
            Publisher publisher = new Publisher(brokerHost, brokerPort, username);
            System.out.println("Connected to broker at " + brokerHost + ":" + brokerPort);

            // Handle publisher commands
            System.out.println("Please select a command: create, publish, show, delete.");
            System.out.println("1. create {topic_id} {topic_name}  #create a new topic");
            System.out.println("2. publish {topic_id} {message}  #publish a message to an existing topic");
            System.out.println("3. show {topic_id}  #show subscriber count for current publisher");
            System.out.println("4. delete {topic_id}  #delete a topic");

            while ((input = reader.readLine()) != null) {
                String[] parts = input.split(" ", 3);

                switch (parts[0]) {
                    case "create":
                        if (parts.length == 3) {
                            String topicId = parts[1];
                            String topicName = parts[2];
                            publisher.createTopic(topicId, topicName);
                            System.out.println(username + " created topic: " + topicName + " (ID: " + topicId + ")");
                        } else {
                            System.out.println("Usage: create {topic_id} {topic_name}");
                        }
                        break;

                    case "publish":
                        if (parts.length == 3) {
                            String topicId = parts[1];
                            String message = parts[2];
                            publisher.publishMessage(topicId, message);
                            System.out.println(username + " published message to topic: " + topicId);
                        } else {
                            System.out.println("Usage: publish {topic_id} {message}");
                        }
                        break;

                    case "show":
                        if (parts.length == 2) {
                            String topicId = parts[1];
                            publisher.showSubscriberCount(topicId);
                        } else {
                            System.out.println("Usage: show {topic_id}");
                        }
                        break;

                    case "delete":
                        if (parts.length == 2) {
                            String topicId = parts[1];
                            publisher.deleteTopic(topicId);
                            System.out.println(username + " deleted topic: " + topicId);
                        } else {
                            System.out.println("Usage: delete {topic_id}");
                        }
                        break;

                    default:
                        System.out.println("Unknown command. Please try again.");
                        break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
