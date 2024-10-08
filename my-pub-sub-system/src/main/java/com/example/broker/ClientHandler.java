package com.example.broker;

import com.example.subscriber.Subscriber;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler extends Thread {
    private final Socket client_socket;
    private final Broker broker;
    private final String username;  // This is captured from the first line sent by the client
    private final String connection_type;  // New variable to store the type of connection
    private PrintWriter out;

    public ClientHandler(Socket socket, Broker broker, String username, String connection_type) {
        this.client_socket = socket;
        this.broker = broker;
        this.username = username;  // Username is passed during connection (from the first message)
        this.connection_type = connection_type;  // Initialize the connection type
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(client_socket.getInputStream()));
             PrintWriter out = new PrintWriter(client_socket.getOutputStream(), true)) {

            this.out = out;
            handle_client_commands(in);

        } catch (IOException e) {
            System.err.println("Client disconnected abruptly: " + client_socket.getInetAddress());
        } finally {
            close_client_socket();
        }
    }

    private void handle_client_commands(BufferedReader in) throws IOException {
        String input_line;
        while ((input_line = in.readLine()) != null) {
            String[] parts = input_line.split(" ", 3);  // Now expects commands with topicId and message as needed

            if (parts.length > 0) {
                String command = parts[0];
                handle_command(command, parts);
            } else {
                out.println("Invalid command.");
            }
        }
    }

    private void handle_command(String command, String[] parts) {
        try {
            switch (connection_type) {
                case "publisher":
                    handle_publisher_commands(command, parts);
                    break;
                case "subscriber":
                    handle_subscriber_commands(command, parts);
                    break;
                case "broker":
                    handle_broker_commands(command, parts);
                    break;
                default:
                    out.println("Unknown connection type.");
            }
        } catch (Exception e) {
            out.println("Error processing command: " + e.getMessage());
        }
    }

    private void handle_publisher_commands(String command, String[] parts) {
        switch (command) {
            case "create":
                handle_create(parts);
                break;
            case "publish":
                handle_publish(parts);
                break;
            default:
                out.println("Invalid command for publisher.");
        }
    }

    private void handle_subscriber_commands(String command, String[] parts) {
        switch (command) {
            case "sub":
                handle_subscribe(parts);
                break;
            case "unsub":
                handle_unsubscribe(parts);
                break;
            case "current":
                handle_current(parts);
                break;
            default:
                out.println("Invalid command for subscriber.");
        }
    }

    private void handle_broker_commands(String command, String[] parts) {
        switch (command) {
            case "synchronize_topic":
                handle_synchronize_topic(parts);
                break;
            case "synchronize_message":
                handle_synchronize_message(parts);
                break;
            case "synchronize_sub":
                handle_synchronize_subscription(parts);
                break;
            case "request_topic":
                handle_request_topic(parts);
                break;
            default:
                out.println("Invalid command for broker.");
        }
    }

    private void handle_create(String[] parts) {
        if (parts.length == 3) {
            broker.createTopic(username, parts[1], parts[2]);
            out.println("Topic created: " + parts[2] + " (ID: " + parts[1] + ")");
        } else {
            out.println("Usage: create {topic_id} {topic_name}");
        }
    }

    private void handle_publish(String[] parts) {
        if (parts.length == 3) {
            broker.publishMessage(username, parts[1], parts[2]);
            out.println("Message published to topic: " + parts[1]);
        } else {
            out.println("Usage: publish {topic_id} {message}");
        }
    }

    private void handle_subscribe(String[] parts) {
        if (parts.length == 2) {
            Subscriber subscriber = new Subscriber(username, out);
            broker.addSubscriber(parts[1], subscriber, username);
            out.println(username + " subscribed to topic: " + parts[1]);
        } else {
            out.println("Usage: sub {topic_id}");
        }
    }

    private void handle_unsubscribe(String[] parts) {
        if (parts.length == 2) {
            broker.unsubscribe(parts[1], username);
            out.println(username + " unsubscribed from topic: " + parts[1]);
        } else {
            out.println("Usage: unsub {topic_id}");
        }
    }

    private void handle_current(String[] parts) {
        broker.listSubscriptions(out, username);
    }

    private void handle_synchronize_topic(String[] parts) {
        if (parts.length == 3) {
            String topic_id = parts[1];
            String topic_name = parts[2];
            broker.createTopic(username, topic_id, topic_name);
            out.println("Synchronized topic: " + topic_name + " (ID: " + topic_id + ")");
        } else {
            out.println("Invalid synchronize_topic message.");
        }
    }

    private void handle_synchronize_message(String[] parts) {
        if (parts.length == 3) {
            String topic_id = parts[1];
            String message = parts[2];
            broker.publishMessage(username, topic_id, message);
            out.println("Synchronized message to topic: " + topic_id);
        } else {
            out.println("Invalid synchronize_message message.");
        }
    }

    private void handle_synchronize_subscription(String[] parts) {
        if (parts.length == 3) {
            String topic_id = parts[1];
            String subscriber_id = parts[2];
            Subscriber subscriber = new Subscriber(subscriber_id, out);
            broker.addSubscriber(topic_id, subscriber, subscriber_id);
            out.println("Synchronized subscription for subscriber: " + subscriber_id + " to topic: " + topic_id);
        } else {
            out.println("Invalid synchronize_sub message.");
        }
    }

    private void handle_request_topic(String[] parts) {
        if (parts.length == 2) {
            String topic_id = parts[1];
            broker.requestTopicFromBrokers(topic_id);
        } else {
            out.println("Invalid request_topic message.");
        }
    }

    private void close_client_socket() {
        try {
            if (!client_socket.isClosed()) {
                client_socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
