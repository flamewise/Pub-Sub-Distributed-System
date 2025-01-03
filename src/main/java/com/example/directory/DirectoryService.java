/**
 * Name: Simon Chen
 * Surname: Chen
 * Student ID: 1196439
 *
 * Description: The DirectoryService class manages the registration and tracking of active brokers in the system. 
 * It allows brokers to register, deregister, and provides a way to retrieve the list of active brokers for 
 * communication between them.
 * 
 * Date: 11/10/2024
 */
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

    // Deregister a broker
    public void deregisterBroker(String brokerAddress) {
        if (activeBrokers.remove(brokerAddress)) {
            System.out.println("Broker deregistered: " + brokerAddress);
        } else {
            System.out.println("Broker not found: " + brokerAddress);
        }
    }
}
