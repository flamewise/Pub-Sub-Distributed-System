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

            out.println("get_brokers");

            String response;
            while ((response = in.readLine()) != null) {
                brokers.add(response);
            }

            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return brokers;
    }
}
