package com.example.broker;

import com.example.directory.DirectoryServiceClient;
import java.util.Set;
import java.util.stream.Collectors;

public class BrokerApp {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java -jar broker.jar <port> <directoryServiceIP:port>");
            return;
        }

        try {
            int port = Integer.parseInt(args[0]);
            String directoryServiceAddress = args[1];  // Directory service address

            Broker broker = new Broker(port);

            // Register the broker with the Directory Service and retrieve the active brokers
            broker.registerWithDirectoryServiceAndConnect(directoryServiceAddress);

            // Use the DirectoryServiceClient to retrieve active brokers
            DirectoryServiceClient directoryServiceClient = new DirectoryServiceClient(directoryServiceAddress);
            Set<String> activeBrokers = directoryServiceClient.getActiveBrokers();

            if (!activeBrokers.isEmpty()) {
                System.out.println("Connecting to active brokers...");
                broker.connectToOtherBrokers(activeBrokers.stream().collect(Collectors.toList()));
            }

            // Start the broker in a new thread
            new Thread(broker::start).start();

        } catch (NumberFormatException e) {
            System.out.println("Error: Port must be a valid number.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
