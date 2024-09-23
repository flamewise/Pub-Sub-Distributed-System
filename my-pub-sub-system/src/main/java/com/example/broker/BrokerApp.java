package com.example.broker;

import java.util.ArrayList;
import java.util.List;

public class BrokerApp {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java -jar broker.jar <port> [<otherBrokerIP:port>...]");
            return;
        }

        try {
            int port = Integer.parseInt(args[0]);
            Broker broker = new BrokerImpl(port);

            // Handle additional broker connections if provided
            List<String> otherBrokers = new ArrayList<>();
            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    otherBrokers.add(args[i]);
                }
            }

            // Start the broker
            new Thread(() -> broker.start()).start();

            // Connect to other brokers if IP:Port are provided
            for (String otherBroker : otherBrokers) {
                String[] brokerDetails = otherBroker.split(":");
                String brokerIP = brokerDetails[0];
                int brokerPort = Integer.parseInt(brokerDetails[1]);

                broker.connectToOtherBroker(brokerIP, brokerPort);  // Method to handle connection to other brokers
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
