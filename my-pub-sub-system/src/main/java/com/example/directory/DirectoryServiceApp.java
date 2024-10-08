package com.example.directory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DirectoryServiceApp {
    
    private final Set<String> activeBrokers = ConcurrentHashMap.newKeySet();

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java -jar directory-service.jar <port>");
            return;
        }

        try {
            int port = Integer.parseInt(args[0]);
            DirectoryServiceApp directoryServiceApp = new DirectoryServiceApp();
            directoryServiceApp.start(port);
        } catch (NumberFormatException e) {
            System.out.println("Error: Port must be a valid number.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Directory Service started on port: " + port);

            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

                    String request = in.readLine();
                    if (request.startsWith("register ")) {
                        String brokerAddress = request.substring(9); // Get the broker address after 'register '
                        registerBroker(brokerAddress);
                        out.println("Broker registered: " + brokerAddress);
                    } else if ("get_brokers".equals(request)) {
                        for (String broker : activeBrokers) {
                            out.println(broker); // Send each broker address
                        }
                        out.println("END"); // End of the list
                    } else if (request.startsWith("deregister ")) {
                        String brokerAddress = request.substring(11); // Get the broker address after 'deregister '
                        deregisterBroker(brokerAddress);
                        out.println("Broker deregistered: " + brokerAddress);
                    }

                    clientSocket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void registerBroker(String brokerAddress) {
        activeBrokers.add(brokerAddress);
        System.out.println("Broker registered: " + brokerAddress);
    }

    public void deregisterBroker(String brokerAddress) {
        if (activeBrokers.remove(brokerAddress)) {
            System.out.println("Broker deregistered: " + brokerAddress);
        } else {
            System.out.println("Broker not found: " + brokerAddress);
        }
    }
}
