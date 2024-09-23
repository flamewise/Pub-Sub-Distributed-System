package com.example;

import com.example.broker.Broker;
import com.example.broker.BrokerImpl;
import com.example.publisher.Publisher;
import com.example.publisher.PublisherImpl;
import com.example.subscriber.Subscriber;
import com.example.subscriber.SubscriberImpl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class App {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java -jar <jarfile> <role> [args...]");
            System.out.println("Roles: broker <port>, publisher <host> <port>, subscriber <host> <port> <subscriberId>");
            return;
        }

        String role = args[0];

        try {
            switch (role) {
                case "broker":
                    // Broker role expects a port argument
                    if (args.length != 2) {
                        System.out.println("Usage: broker <port>");
                        return;
                    }
                    int brokerPort = Integer.parseInt(args[1]);
                    Broker broker = new BrokerImpl(brokerPort);  // Initialize the broker
                    new Thread(() -> broker.start()).start();     // Run the broker in a separate thread
                    break;

                case "publisher":
                    // Publisher role expects a host and port
                    if (args.length != 3) {
                        System.out.println("Usage: publisher <host> <port>");
                        return;
                    }
                    String host = args[1];
                    int port = Integer.parseInt(args[2]);
                    Publisher publisher = new PublisherImpl(host, port);
                    
                    // Handle user input for publishing messages
                    BufferedReader consoleReader = new BufferedReader(new InputStreamReader(System.in));
                    String input;

                    // Regular expression for the publish command: publish <topic> <message>
                    Pattern pattern = Pattern.compile("^publish\\s+(\\S+)\\s+(.+)$");

                    System.out.println("Enter commands (e.g., publish <topic_id> <message>):");
                    while ((input = consoleReader.readLine()) != null) {
                        Matcher matcher = pattern.matcher(input);

                        if (matcher.matches()) {
                            String topic = matcher.group(1);
                            String message = matcher.group(2);
                            publisher.publishMessage(topic, message);
                            System.out.println("Message published to " + topic);
                        } else {
                            System.out.println("Unknown command or invalid format. Use: publish <topic_id> <message>");
                        }
                    }
                    break;

                case "subscriber":
                    // Subscriber role expects a host, port, and subscriber ID
                    if (args.length != 4) {
                        System.out.println("Usage: subscriber <host> <port> <subscriberId>");
                        return;
                    }
                    String subHost = args[1];
                    int subPort = Integer.parseInt(args[2]);
                    String subscriberId = args[3];
                    Subscriber subscriber = new SubscriberImpl(subHost, subPort, subscriberId);

                    // The subscriber can also be used to subscribe to topics here
                    BufferedReader subscriberReader = new BufferedReader(new InputStreamReader(System.in));
                    String subInput;
                    System.out.println("Enter commands (e.g., sub <topic_id>):");
                    while ((subInput = subscriberReader.readLine()) != null) {
                        String[] subParts = subInput.split(" ", 2);
                        if (subParts.length == 2 && "sub".equals(subParts[0])) {
                            subscriber.subscribe(subParts[1]);
                        } else if (subParts.length == 2 && "unsub".equals(subParts[0])) {
                            subscriber.unsubscribe(subParts[1]);
                        } else {
                            System.out.println("Unknown command or invalid format. Use: sub <topic_id> or unsub <topic_id>");
                        }
                    }
                    break;

                default:
                    System.out.println("Unknown role: " + role);
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
