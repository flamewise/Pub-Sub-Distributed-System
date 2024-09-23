package com.example.subscriber;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class SubscriberApp {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java -jar subscriber.jar <host> <port> <subscriberId>");
            return;
        }
        try {
            String host = args[0];
            int port = Integer.parseInt(args[1]);
            String subscriberId = args[2];
            Subscriber subscriber = new SubscriberImpl(host, port, subscriberId);

            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String input;
            System.out.println("Enter commands (e.g., sub <topic>):");
            while ((input = reader.readLine()) != null) {
                String[] parts = input.split(" ", 2);
                if (parts.length == 2 && "sub".equals(parts[0])) {
                    subscriber.subscribe(parts[1]);
                } else if (parts.length == 2 && "unsub".equals(parts[0])) {
                    subscriber.unsubscribe(parts[1]);
                } else {
                    System.out.println("Unknown command. Use: sub <topic> or unsub <topic>");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
