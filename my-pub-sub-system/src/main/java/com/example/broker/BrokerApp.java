package com.example.broker;

import com.example.directory.DirectoryServiceClient;

import java.util.Set;
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

            // Retrieve the list of active brokers from the Directory Service
            DirectoryServiceClient directoryServiceClient = new DirectoryServiceClient(directoryServiceAddress);
            Set<String> activeBrokers = directoryServiceClient.getActiveBrokers();

            // Connect to all active brokers
            if (!activeBrokers.isEmpty()) {
                System.out.println("Connecting to active brokers from the Directory Service...");
                for (String brokerAddress : activeBrokers) {
                    if (!broker.getConnectedBrokerAddresses().contains(brokerAddress)) {
                        String[] brokerDetails = brokerAddress.split(":");
                        String brokerIP = brokerDetails[0];
                        int brokerPort = Integer.parseInt(brokerDetails[1]);
                        broker.connectToBroker(brokerIP, brokerPort);
                    }
                }
            } else {
                System.out.println("No other active brokers available.");
            }

            // Handle additional broker connections if provided using the "-b" flag
            List<String> otherBrokers = broker.parseBrokerAddresses(args);

            // Start the broker in a new thread
            new Thread(broker::start).start();

            // Connect to other brokers if IP:Port are provided
            if (!otherBrokers.isEmpty()) {
                System.out.println("Connecting to manually provided brokers...");
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
