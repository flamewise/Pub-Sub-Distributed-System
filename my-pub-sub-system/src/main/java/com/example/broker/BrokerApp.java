package com.example.broker;

import com.example.directory.DirectoryService;

import java.util.List;

public class BrokerApp {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java -jar broker.jar <port> <directoryServiceIP:port> [-b <brokerIP:port> ...]");
            return;
        }

        try {
            int port = Integer.parseInt(args[0]);
            String directoryServiceAddress = args[1];  // Directory service address

            Broker broker = new Broker(port);

            // Register with Directory Service
            broker.registerWithDirectoryService(directoryServiceAddress);

            // Handle additional broker connections if provided using the "-b" flag
            List<String> otherBrokers = broker.parseBrokerAddresses(args);

            // Start the broker in a new thread
            new Thread(broker::start).start();

            // Connect to other brokers if IP:Port are provided
            if (!otherBrokers.isEmpty()) {
                System.out.println("Connecting to other brokers...");
                broker.connectToOtherBrokers(otherBrokers);
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
