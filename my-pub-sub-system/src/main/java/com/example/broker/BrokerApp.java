/**
 * Name: Simon Chen
 * Surname: Chen
 * Student ID: 1196439
 *
 * Description: The BrokerApp class is the main entry point for running the broker application. It sets up a new Broker instance 
 * and starts it in a separate thread. The broker is initialized with a port number and a directory service address 
 * that helps it discover and communicate with other brokers in the network.
 * 
 * Date: 11/10/2024
 */

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
