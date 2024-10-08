package com.example.subscriber;

import com.example.directory.DirectoryServiceClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class SubscriberApp {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java -jar subscriber.jar <username> <directory_service_ip> <directory_service_port>");
            return;
        }

        try {
            String username = args[0];
            String directoryServiceIP = args[1];
            String directoryServicePort = args[2];

            // Initialize DirectoryServiceClient to fetch available brokers
            DirectoryServiceClient directoryServiceClient = new DirectoryServiceClient(directoryServiceIP + ":" + directoryServicePort);
            List<String> brokerAddresses = new ArrayList<>(directoryServiceClient.getActiveBrokers());

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
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Send the username and connection type to the broker after connection
            out.println(username + " subscriber");

            // Create Subscriber object
            Subscriber subscriber = new Subscriber(username, out);

            // Start a thread to listen for messages from the broker
            new Thread(() -> {
                try {
                    String brokerMessage;
                    while ((brokerMessage = in.readLine()) != null) {
                        System.out.println("Broker: " + brokerMessage);
                    }
                } catch (Exception e) {
                    System.err.println("Connection closed by broker.");
                }
            }).start();

            // Read user input and send commands to the broker
            System.out.println("Enter commands (list all, sub <topic_id>, current, unsub <topic_id>, exit):");

            while ((input = reader.readLine()) != null) {
                String[] parts = input.split(" ", 2);

                switch (parts[0]) {
                    case "sub":
                        if (parts.length == 2) {
                            subscriber.subscribe(parts[1]);  // Subscribe to a topic
                            System.out.println("Subscribed to topic: " + parts[1]);
                        } else {
                            System.out.println("Usage: sub <topic_id>");
                        }
                        break;

                    case "unsub":
                        if (parts.length == 2) {
                            subscriber.unsubscribe(parts[1]);  // Unsubscribe from a topic
                            System.out.println("Unsubscribed from topic: " + parts[1]);
                        } else {
                            System.out.println("Usage: unsub <topic_id>");
                        }
                        break;

                    case "list":
                        if (parts.length == 2 && "all".equals(parts[1])) {
                            out.println("list all");  // Request list of all topics
                            System.out.println("Requested list of all topics.");
                        } else {
                            System.out.println("Usage: list all");
                        }
                        break;

                    case "current":
                        out.println("current");  // Request the current subscriptions for this subscriber
                        System.out.println("Requested list of current subscriptions.");
                        break;

                    case "exit":
                        out.println("exit");  // Close the connection and exit
                        socket.close();
                        System.out.println("Exiting...");
                        return;

                    default:
                        System.out.println("Unknown command. Use: list all, sub <topic_id>, current, unsub <topic_id>, or exit.");
                        break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
