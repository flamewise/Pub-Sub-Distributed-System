package com.example.broker;

import java.util.ArrayList;
import java.util.List;

public class BrokerApp {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java -jar broker.jar <port> [-b <brokerIP:port> ...]");
            return;
        }

        try {
            int port = Integer.parseInt(args[0]);
            Broker broker = new Broker(port);

            // Handle additional broker connections if provided using the "-b" flag
            List<String> otherBrokers = new ArrayList<>();
            if (args.length > 2 && "-b".equals(args[1])) {
                for (int i = 2; i < args.length; i++) {
                    otherBrokers.add(args[i]);
                }
            }

            // Start the broker in a new thread
            new Thread(broker::start).start();

            // Connect to other brokers if IP:Port are provided
            if (!otherBrokers.isEmpty()) {
                System.out.println("Connecting to other brokers...");
                for (String otherBroker : otherBrokers) {
                    String[] brokerDetails = otherBroker.split(":");
                    if (brokerDetails.length == 2) {
                        String brokerIP = brokerDetails[0];
                        int brokerPort = Integer.parseInt(brokerDetails[1]);

                        broker.connectToBroker(brokerIP, brokerPort);  // Connect to the specified broker
                    } else {
                        System.out.println("Invalid broker address: " + otherBroker);
                    }
                }
            } else {
                System.out.println("No other brokers to connect to.");
            }

        } catch (NumberFormatException e) {
            System.out.println("Error: Port must be a valid number.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
