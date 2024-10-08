package com.example.directory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

public class DirectoryServiceClient {
    private final String directoryServiceAddress;

    public DirectoryServiceClient(String directoryServiceAddress) {
        this.directoryServiceAddress = directoryServiceAddress;
    }

    public Set<String> getActiveBrokers() {
        Set<String> brokers = new HashSet<>();
        try {
            String[] addressParts = directoryServiceAddress.split(":");
            String dirServiceIP = addressParts[0];
            int dirServicePort = Integer.parseInt(addressParts[1]);
    
            Socket socket = new Socket(dirServiceIP, dirServicePort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    
            // Send the request to get the active brokers
            System.out.println("Requesting active brokers from Directory Service...");
            out.println("get_brokers");
    
            // Read the response and collect brokers
            String response;
            System.out.println("Active brokers retrieved from Directory Service:");
            while ((response = in.readLine()) != null) {
                if ("END".equals(response)) {
                    break;
                }
                brokers.add(response);
                System.out.println(" - " + response);  // Print each broker as it is added
            }
    
            socket.close();
        } catch (IOException e) {
            System.out.println("Error retrieving active brokers from Directory Service: " + e.getMessage());
        }
        return brokers;
    }
    
}
