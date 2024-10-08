package com.example.subscriber;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class SubscriberApp {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java -jar subscriber.jar <host> <port> <username>");
            return;
        }

        try {
            String host = args[0];
            int port = Integer.parseInt(args[1]);
            String username = args[2];

            // Establish a connection to the broker
            Socket socket = new Socket(host, port);
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
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String input;
            System.out.println("Enter commands (list all, sub <topic_id>, current, unsub <topic_id>, exit):");

            while ((input = reader.readLine()) != null) {
                System.out.println("Socket Message from Broker" + input);
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
