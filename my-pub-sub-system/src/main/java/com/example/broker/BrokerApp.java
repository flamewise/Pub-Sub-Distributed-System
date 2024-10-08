package com.example.broker;

public class BrokerApp {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java -jar broker.jar <port> <directoryServiceIP:port>");
            return;
        }

        try {
            int port = Integer.parseInt(args[0]);
            String directoryServiceAddress = args[1];

            // Create a new Broker instance with the port and directory service address
            Broker broker = new Broker(port, directoryServiceAddress);

            // Start the broker in a new thread using a Runnable
            Thread brokerThread = new Thread(() -> broker.start());
            brokerThread.start();

        } catch (NumberFormatException e) {
            System.out.println("Error: Port must be a valid number.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
