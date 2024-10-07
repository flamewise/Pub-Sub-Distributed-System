package com.example.publisher;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class PublisherApp {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java -jar publisher.jar <username> <broker_ip> <broker_port>");
            return;
        }

        try {
            String username = args[0];
            String host = args[1];
            int port = Integer.parseInt(args[2]);
            Publisher publisher = new Publisher(host, port, username);

            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String input;
            System.out.println("Connected to broker at " + host + ":" + port + " as " + username);
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
                            publisher.createTopic(topicId, topicName);  // Send topicId, and topicName
                            System.out.println(username + " created topic: " + topicName + " (ID: " + topicId + ")");
                        } else {
                            System.out.println("Usage: create {topic_id} {topic_name}");
                        }
                        break;

                    case "publish":
                        if (parts.length == 3) {
                            String topicId = parts[1];
                            String message = parts[2];
                            publisher.publishMessage(topicId, message);  // Send topicId, and message
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
