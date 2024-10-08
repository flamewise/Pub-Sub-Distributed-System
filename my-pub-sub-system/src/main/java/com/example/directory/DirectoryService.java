package com.example.directory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DirectoryService {
    private final Set<String> activeBrokers = ConcurrentHashMap.newKeySet();  // Store broker IP:port addresses

    // Broker registration
    public void registerBroker(String brokerAddress) {
        activeBrokers.add(brokerAddress);
        System.out.println("Broker registered: " + brokerAddress);
    }

    // Get all active brokers
    public Set<String> getActiveBrokers() {
        return activeBrokers;
    }
}
