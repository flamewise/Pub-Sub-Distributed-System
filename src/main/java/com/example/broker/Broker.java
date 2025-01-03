/**
 * Name: Simon Chen
 * Surname: Chen
 * Student ID: 1196439
 *
 * Description: The Broker class represents a message broker that handles publisher-subscriber communication.
 * It maintains topics, manages subscriptions, and facilitates message distribution to subscribers. The class
 * also handles broker-to-broker communication to synchronize messages and topics across the network.
 * 
 * Date: 11/10/2024
 */

package com.example.broker;

import com.example.subscriber.Subscriber;
import com.example.directory.DirectoryServiceClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Broker {
    private final String ownBrokerAddress;  // Store the broker's own address
    public final ConcurrentHashMap<String, ConcurrentHashMap<String, Subscriber>> topicSubscribers; // topicId -> (username -> Subscriber)
    public final ConcurrentHashMap<String, String> topicNames; // topicId -> topicName
    public final ConcurrentHashMap<String, String> topicPublishers;  // topicId -> publisherUsername
    private final ConcurrentHashMap<String, String> subscriberUsernames;  // Map username to topicId
    private final Set<String> connectedBrokerAddresses = ConcurrentHashMap.newKeySet(); // Stores connected brokers
    private final CopyOnWriteArrayList<Socket> connectedBrokers;
    private final ExecutorService connectionPool;
    private final DirectoryServiceClient directoryServiceClient;
    private final ServerSocket serverSocket;
    private final CopyOnWriteArrayList<ClientHandler> subClientHandlers;
    private final CopyOnWriteArrayList<ClientHandler> pubClientHandlers;
    private final CopyOnWriteArrayList<BrokerHandler> brokerBrokerHandlers;
    
    private boolean isLocked;
    private int TOTAL_SUB_LIMIT = 10;
    private int TOTAL_PUB_LIMIT = 5;


    public Broker(int port, String directoryServiceAddress) throws IOException {
        this.topicSubscribers = new ConcurrentHashMap<>();
        this.topicNames = new ConcurrentHashMap<>();
        this.topicPublishers = new ConcurrentHashMap<>();
        this.subscriberUsernames = new ConcurrentHashMap<>();
        this.connectedBrokers = new CopyOnWriteArrayList<>();
        this.connectionPool = Executors.newCachedThreadPool();
        this.directoryServiceClient = new DirectoryServiceClient(directoryServiceAddress);
        this.serverSocket = new ServerSocket(port);
        this.ownBrokerAddress = serverSocket.getInetAddress().getHostAddress() + ":" + port;
        this.subClientHandlers = new CopyOnWriteArrayList<>();
        this.pubClientHandlers = new CopyOnWriteArrayList<>();
        this.brokerBrokerHandlers = new CopyOnWriteArrayList<>();
        this.isLocked = false;
        System.out.println("Broker started on port: " + port);

        // Register the broker with the directory service
        directoryServiceClient.registerBroker(ownBrokerAddress);
        System.out.println("Broker registered with Directory Service at: " + directoryServiceAddress);
    }


    public void start() {
        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

                // Perform handshake before any further communication
                if (!waitForHandshake(in, out, clientSocket)) {
                    System.out.println("Handshake failed, closing connection.");
                    clientSocket.close();
                    continue;
                }

                // At this point, the handshake is complete, and the client is identified
                System.out.println("Handshake successful!");

                // The client type and username have already been captured in waitForHandshake,
                // and now we handle client logic based on the connection type.
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean waitForHandshake(BufferedReader in, PrintWriter out, Socket clientSocket) throws IOException {
        System.out.println("Waiting for HANDSHAKE_INIT from client...");
    
        // Receive the handshake initiation
        String handshakeInit = in.readLine();
        if (handshakeInit == null || !handshakeInit.startsWith("HANDSHAKE_INIT")) {
            System.err.println("Invalid handshake initiation from client. Should recerive HANDSHAKE_INIT but we got " + handshakeInit);
            return false;
        }
    
        // Extract the username (or broker address) and connection type from the message
        String[] parts = handshakeInit.split(" ", 3);
        if (parts.length != 3 || !"HANDSHAKE_INIT".equals(parts[0])) {
            System.err.println("Invalid handshake format. Expected: HANDSHAKE_INIT <username> <connectionType>");
            return false;
        }
    
        String username = parts[1];
        String connectionType = parts[2];
        System.out.println("Received handshake initiation from: " + username + " as " + connectionType);
    
        // Depending on the connection type, handle the client or broker connection
        if ("broker".equals(connectionType)) {
            System.out.println("Broker connected: " + username);
            // Add broker to the list of connected brokers
            connectedBrokers.add(clientSocket);
            connectedBrokerAddresses.add(username);
            // Submit the broker to the broker handler for further processing
            BrokerHandler brokerHandler = new BrokerHandler(clientSocket, this, username);
            brokerBrokerHandlers.add(brokerHandler);
            connectionPool.submit(brokerHandler);
        } else if ("publisher".equals(connectionType) || "subscriber".equals(connectionType)) {
            // Handle publisher or subscriber connection
            ClientHandler clientHandler = new ClientHandler(clientSocket, this, username, connectionType);
            
            if ("subscriber".equals(connectionType)) {
                boolean lock = requestLockFromAllBrokers();
                if (!lock) {
                    //out.println("Handshake refused, failed to acquire lock");
                    return false;
                }

                if (getTotalSubscriberCount() >= TOTAL_SUB_LIMIT) {
                    releaseLockFromAllBrokers();
                    return false;
                }
                subClientHandlers.add(clientHandler);
                releaseLockFromAllBrokers();
            } else if ("publisher".equals(connectionType)) {
                boolean lock = requestLockFromAllBrokers();
                if (!lock) {
                    //out.println("Handshake refused, failed to acquire lock");
                    return false;
                }
                if (getTotalPublisherCount() >= TOTAL_PUB_LIMIT) {
                    releaseLockFromAllBrokers();
                    return false;
                }
                pubClientHandlers.add(clientHandler);
                releaseLockFromAllBrokers();
            }
            connectionPool.submit(clientHandler);
            System.out.println("Client connected as: " + connectionType);
        } else {
            System.out.println("Connection not know " + connectionType);
            System.err.println("Unknown connection type: " + connectionType);
            return false;
        }
    
        // Once everything is validated, send the handshake acknowledgment
        out.println("HANDSHAKE_ACK");
        out.flush();  // Ensure the message is sent
        System.out.println("Sent HANDSHAKE_ACK to client: " + username);
    
        return true;
    }
    

    public void createTopic(String username, String topicId, String topicName) {
        if (!topicNames.containsKey(topicId)) {
            topicSubscribers.putIfAbsent(topicId, new ConcurrentHashMap<>());
            topicNames.put(topicId, topicName);
            topicPublishers.put(topicId, username);
            System.out.println(username + " created topic: " + topicName + " (ID: " + topicId + ")");
    
            // Synchronize the newly created topic with all brokers, passing the username
            synchronizeTopic(username, topicId, topicName);
        } else {
            System.out.println("Topic already exists: " + topicNames.get(topicId));
        }
    }
    
    public boolean topicExists(String topicId) {
        return topicNames.containsKey(topicId);
    }
    
    public boolean isTopicOwner(String topicId, String username) {
        String owner = topicPublishers.get(topicId);
        return owner != null && owner.equals(username);
    }
    

    // Only add topic topicid topicname, no further function call, will be used in handlesynchronized
    public void createSimpleTopic(String username, String topicId, String topicName) {
        // Ensure the topicId does not already exist
        if (!topicNames.containsKey(topicId)) {
            // Store the topicId, topicName, and associated publisher username
            topicSubscribers.putIfAbsent(topicId, new ConcurrentHashMap<>());
            topicNames.put(topicId, topicName);
            topicPublishers.put(topicId, username);
    
            System.out.println("Topic created by " + username + ": " + topicName + " (ID: " + topicId + ")");
        } else {
            System.out.println("Topic already exists: " + topicName + " (ID: " + topicId + ")");
        }
    }
    
    
    public void publishMessage(String topicId, String message, boolean synchronizedRequired) {
        // Check if the topic exists and has subscribers
        if (topicSubscribers.containsKey(topicId)) {
            // Retrieve the topic name
            String topicName = topicNames.get(topicId);
            
            // Check each subscriber in subClientHandlers
            for (ClientHandler clientHandler : subClientHandlers) {
                String subscriberUsername = clientHandler.getUserName();
                ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);
                
                // Check if the subscriber is subscribed to the topic
                if (subscribers != null && subscribers.containsKey(subscriberUsername)) {
                    try {
                        // Send the message to the subscriber
                        PrintWriter out = new PrintWriter(clientHandler.getClientSocket().getOutputStream(), true);
                        String timestamp = new java.text.SimpleDateFormat("dd/MM HH:mm:ss").format(new java.util.Date());
                        out.println(timestamp + " " + topicId + ":" + this.topicNames.get(topicId) + ": " + "Message Received: "  + message);
                        out.flush();
                        System.out.println("Message sent to subscriber: " + subscriberUsername + " on topic: " + topicId);
                    } catch (IOException e) {
                        System.err.println("Error sending message to subscriber: " + subscriberUsername);
                        e.printStackTrace();
                    }
                }
            }
    
            // If synchronization is required, inform other brokers
            if (synchronizedRequired) {
                synchronizeMessage(topicId, message);
            }
    
        } else {
            System.out.println("Topic not found: " + topicId);
        }
    }
    

    public void addSubscriberId(String topicId, String subscriberId, boolean synchronizedRequired) {
        // Check if the topic already has a subscriber list; if not, create one
        ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.computeIfAbsent(topicId, k -> new ConcurrentHashMap<>());
        
        // Create a new Subscriber object if it does not already exist
        if (!subscribers.containsKey(subscriberId)) {
            // Create the Subscriber object (you can modify it to store more meaningful data)
            Subscriber subscriber = new Subscriber(subscriberId, new PrintWriter(System.out, true), null); // Placeholder for the real writer/reader
            subscribers.put(subscriberId, subscriber);
            
            if (synchronizedRequired) {
                // Synchronize the subscription across all brokers immediately
                synchronizeSubscription(topicId, subscriberId);
            }
        }
    }
    


    public void synchronizeSubscription(String topicId, String subscriberId) {
        // Update the list of connected brokers before synchronization
        this.updateConnectedBrokers();
        
        // Synchronize subscription with all connected brokers
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                // Send the synchronization message to the connected broker
                out.println("synchronize_sub " + topicId + " " + subscriberId);
                System.out.println("synchronize_sub " + topicId + " " + subscriberId);
                System.out.println("dowqodnqwd");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    
    public void unsubscribe(String topicId, String username, boolean synchronizedRequired) {
        ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);
        if (subscribers != null && subscribers.containsKey(username)) {
            subscribers.remove(username);
            subscriberUsernames.remove(username);
            System.out.println(username + " unsubscribed from topic: " + topicId);
    
            // If synchronization is required, synchronize the unsubscription across brokers
            if (synchronizedRequired) {
                synchronizeUnsubscription(topicId, username);
            }
        } else {
            System.out.println("Unsubscription failed: No subscription found for " + username + " on topic " + topicId);
        }
    }
    

    public void synchronizeUnsubscription(String topicId, String subscriberId) {
        // Update the list of connected brokers before synchronization
        this.updateConnectedBrokers();
    
        // Synchronize unsubscription with all connected brokers
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                // Send the synchronization message to the connected broker
                out.println("synchronize_unsub " + topicId + " " + subscriberId);
                out.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
      
    

    // Method to list all topics to a subscriber
    public void listAllTopics(PrintWriter out) {
        updateConnectedBrokers();
        if (topicNames.isEmpty()) {
            out.println("No topics available.");
        } else {
            for (String topicId : topicNames.keySet()) {
                String topicName = topicNames.get(topicId);
                String publisherName = topicPublishers.get(topicId);  // Get the publisher name
                //String timestamp = new java.text.SimpleDateFormat("dd/MM HH:mm:ss").format(new java.util.Date());
                out.println("Topic ID: " + topicId + ", Name: " + topicName + ", Publisher: " + publisherName);
            }
        }
        out.println("END");  // Indicate the end of the topic list
    }


    public void listSubscriptions(PrintWriter out, String subscriberId) {
        updateConnectedBrokers();
        boolean hasSubscribeTopic = false;
        for (String topicId : topicSubscribers.keySet()) {
            ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);
            if (subscribers.containsKey(subscriberId)) {
                hasSubscribeTopic = true;
                //String timestamp = new java.text.SimpleDateFormat("dd/MM HH:mm:ss").format(new java.util.Date());
                out.println("success: " + "Subscribed to: " + topicId + " (" + topicNames.get(topicId) + ")");
            }
        }

        if (!hasSubscribeTopic){
            //String timestamp = new java.text.SimpleDateFormat("dd/MM HH:mm:ss").format(new java.util.Date());
            out.println("No any subscribed topic");
        }
        out.println("END");
    }

    public void synchronizeTopic(String username, String topicId, String topicName) {
        updateConnectedBrokers();
        Set<String> activeBrokers = directoryServiceClient.getActiveBrokers();
    
        for (String brokerAddress : activeBrokers) {
            if (!connectedBrokerAddresses.contains(brokerAddress)) {
                connectToBroker(brokerAddress);
            }
        }
    
        // Print debug info and send synchronization message to all connected brokers
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
    
                // Print full socket information: Local and Remote addresses and ports
                String socketIP = brokerSocket.getInetAddress().getHostAddress();
                int socketPort = brokerSocket.getPort();
                String localAddress = brokerSocket.getLocalAddress().toString();
                int localPort = brokerSocket.getLocalPort();
                String remoteAddress = brokerSocket.getRemoteSocketAddress().toString();
    
                System.out.println("Sending to broker at IP: " + socketIP + " Port: " + socketPort +
                                   ". Full socket info: Local Address: " + localAddress + 
                                   " Local Port: " + localPort + 
                                   " Remote Address: " + remoteAddress);
    
                // Send the synchronization message with the username
                out.println("synchronize_topic " + topicId + " " + topicName + " " + username);
                out.flush();
                System.out.println("Sent 'synchronize_topic' for topic ID: " + topicId + " with username: " + username + " to broker at IP: " + socketIP + " Port: " + socketPort);
    
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    

    public void synchronizeMessage(String topicId, String message) {
        updateConnectedBrokers();
        Set<String> activeBrokers = directoryServiceClient.getActiveBrokers();
        for (String brokerAddress : activeBrokers) {
            if (!connectedBrokerAddresses.contains(brokerAddress)) {
                connectToBroker(brokerAddress);
            }
        }

        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("synchronize_message " + topicId + " " + message);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void connectToBroker(String brokerAddress) {
        String[] parts = brokerAddress.split(":");
        String brokerIP = parts[0];
        int brokerPort = Integer.parseInt(parts[1]);
    
        // Check if the brokerAddress is the same as this broker's own address
        if (brokerAddress.equals(ownBrokerAddress)) {
            System.out.println("Skipping connection to self at: " + brokerAddress);
            return;  // Skip connecting to itself
        }
    
        if (connectedBrokerAddresses.contains(brokerAddress)) {
            System.out.println("Already connected to broker at: " + brokerAddress);
            return;
        }
    
        try {
            // Establish the connection synchronously
            Socket brokerSocket = new Socket(brokerIP, brokerPort);
            connectedBrokers.add(brokerSocket);
            connectedBrokerAddresses.add(brokerAddress);
            PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(brokerSocket.getInputStream()));
    
            // Perform the handshake with the broker
            if (!performBrokerHandshake(out, in, brokerSocket)) {
                System.err.println("Handshake with broker failed: " + brokerAddress + ". Closing connection.");
                brokerSocket.close();
                return;
            }
    
            System.out.println("Broker handshake successful with " + brokerAddress);
    
            // Now submit the BrokerHandler task to handle the broker communication asynchronously
            BrokerHandler brokerHandler = new BrokerHandler(brokerSocket, this, brokerIP + ":" + brokerPort);
            connectionPool.submit(brokerHandler);
            brokerBrokerHandlers.add(brokerHandler);
        } catch (IOException e) {
            System.out.println("Error connecting to broker at " + brokerAddress + ": " + e.getMessage());
        }
    }
    
    private boolean performBrokerHandshake(PrintWriter out, BufferedReader in, Socket brokerSocket) throws IOException {
        // Retrieve and print socket information
        String localAddress = brokerSocket.getLocalAddress().toString();
        int localPort = brokerSocket.getLocalPort();
        String remoteAddress = brokerSocket.getRemoteSocketAddress().toString();
        System.out.println("Local Address: " + localAddress + ", Local Port: " + localPort);
        System.out.println("Remote Address: " + remoteAddress);
    
        // Send handshake initiation message to the broker
        out.println("HANDSHAKE_INIT " + ownBrokerAddress + " broker");
        out.flush();  // Ensure the message is sent
        System.out.println("Sent HANDSHAKE_INIT to broker at IP: " + remoteAddress);
    
        // Wait for the broker to respond with a handshake acknowledgment
        String ack = in.readLine();
        System.out.println("Received from broker: " + ack);
    
        // Check if the handshake was successful
        if ("HANDSHAKE_ACK".equals(ack)) {
            System.out.println("Broker handshake successful with broker at IP: " + remoteAddress);
            return true;
        } else {
            System.err.println("Broker handshake failed with broker at IP: " + remoteAddress + ". Received: " + ack);
            return false;
        }
    }
     

    public void updateConnectedBrokers() {
        // Retrieve active brokers from the Directory Service
        Set<String> activeBrokers = directoryServiceClient.getActiveBrokers();
    
        // For each broker in the active broker list
        for (String brokerAddress : activeBrokers) {
            // If we are not already connected to this broker, establish a connection
            if (!connectedBrokerAddresses.contains(brokerAddress)) {
                connectToBroker(brokerAddress); // This method will handle the connection
            }
        }
    }
    public void showSubscriberCount(String topicId, PrintWriter out) {
        ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);
        String timestamp = new java.text.SimpleDateFormat("dd/MM HH:mm:ss").format(new java.util.Date());
        if (subscribers != null) {
            out.println("Subscriber count for topic " + topicId + ": " + subscribers.size());
        } else {
            
            out.println(timestamp + " Topic not found: " + topicId);
        }
    }

    public void deleteTopic(String topicId, boolean synchronizedRequired) {
        // Check if the topic exists
        if (topicNames.containsKey(topicId)) {
            // Remove the topic from the system
            String topicName = topicNames.remove(topicId);
            topicPublishers.remove(topicId);
            
            // Find all subscribers subscribed to the topic
            ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.remove(topicId);
    
            // Notify and unsubscribe all subscribers
            if (subscribers != null && !subscribers.isEmpty()) {
                for (ClientHandler clientHandler : subClientHandlers) {
                    String subscriberUsername = clientHandler.getUserName();
    
                    // Check if the client handler is subscribed to the topic
                    if (subscribers.containsKey(subscriberUsername)) {
                        try {
                            // Send the topic deletion message to the subscriber with the formatted timestamp and topic details
                            PrintWriter out = new PrintWriter(clientHandler.getClientSocket().getOutputStream(), true);
                            // Get the current date and time in the desired format: dd/MM hh:mm:ss
                            //String timestamp = new java.text.SimpleDateFormat("dd/MM HH:mm:ss").format(new java.util.Date());
                            out.println( "Topic " + topicId + " (" + topicName + ") has been deleted.");
                            out.flush();
                            System.out.println("Notified subscriber " + subscriberUsername + " about the deletion of topic: " + topicId);
                        } catch (IOException e) {
                            System.err.println("Error notifying subscriber: " + subscriberUsername);
                            e.printStackTrace();
                        }
                        
                        // Remove the subscription
                        subscriberUsernames.remove(subscriberUsername);
                    }
                }
            }
    
            System.out.println("Topic " + topicId + " deleted.");
    
            // If synchronization is required, notify other brokers
            if (synchronizedRequired) {
                synchronizeDelete(topicId);
            }
        } else {
            System.out.println("Delete failed: Topic not found.");
        }
    }
    
    
    public void synchronizeDelete(String topicId) {
        updateConnectedBrokers();
    
        // Synchronize topic deletion with all connected brokers
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("synchronize_delete " + topicId);
                out.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    //If broker want to connect to publisher/subscriber, it need to obtain the lock and check current connected publisher/subscriber across broker network
    // Method to lock the broker
    public synchronized void lock() {
        isLocked = true;
        System.out.println("Broker locked. No new connections can be accepted.");
    }

    // Method to unlock the broker
    public synchronized void unlock() {
        isLocked = false;
        System.out.println("Broker unlocked. New connections can now be accepted.");
    }

    // Method to check if the broker is locked
    public synchronized boolean isLocked() {
        return isLocked;
    }

    private boolean requestLockFromAllBrokers() {
        updateConnectedBrokers();
        boolean lockAcquired = true;
        List<Socket> successfullyLockedBrokers = new ArrayList<>();
    
        for (BrokerHandler brokerHandler : brokerBrokerHandlers) {
            Socket brokerSocket = brokerHandler.getSocket();  // Get the broker's socket
    
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("request_lock");  // Send lock request
                out.flush();
    
                // Poll the response map in BrokerHandler to get the lock acknowledgment
                String response;
                while ((response = brokerHandler.getResponse("request_lock")) == null) {
                    Thread.sleep(50);  // Short delay before re-checking
                }
    
                // Check if the response is "lock_ack"
                if (!"lock_ack".equals(response)) {
                    lockAcquired = false;
                    break;
                }
    
                // If lock was acquired, add the broker to the list of locked brokers
                successfullyLockedBrokers.add(brokerSocket);
    
            } catch (IOException | InterruptedException e) {
                System.err.println("Error requesting lock from broker: " + e.getMessage());
                lockAcquired = false;
                break;
            }
        }
    
        // If failed to acquire the lock, release all locks acquired so far
        if (!lockAcquired) {
            releaseLocksFromBrokers(successfullyLockedBrokers);
        }
    
        return lockAcquired;
    }
    

    private void releaseLocksFromBrokers(List<Socket> lockedBrokers) {
        for (Socket brokerSocket : lockedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                // Send unlock command
                out.println("release_lock");
                out.flush();
            } catch (IOException e) {
                System.err.println("Error releasing lock from broker: " + e.getMessage());
            }
        }
    }


    private void releaseLockFromAllBrokers() {
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                // Send lock release message
                out.println("release_lock");
                out.flush();
            } catch (IOException e) {
                System.err.println("Error releasing lock from broker: " + e.getMessage());
            }
        }
    
        // Also unlock the current broker
        this.unlock();
    }
    
    public int getLocalSubscriberCount() {
        return this.subClientHandlers.size();
    }

    public int getTotalSubscriberCount() {
        int totalSubscriberCount = getLocalSubscriberCount();  // Start with the local subscriber count
    
        for (BrokerHandler brokerHandler : brokerBrokerHandlers) {
            Socket brokerSocket = brokerHandler.getSocket();  // Get the broker's socket
    
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                
                // Send a request to the broker to get its local subscriber count
                out.println("get_local_subscriber_count");
                out.flush();
    
                // Poll the response map in BrokerHandler to get the result
                String response;
                while ((response = brokerHandler.getResponse("get_local_subscriber_count")) == null) {
                    Thread.sleep(50); // Short delay before re-checking
                }
    
                // Parse the response as an integer
                try {
                    int brokerSubscriberCount = Integer.parseInt(response);
                    totalSubscriberCount += brokerSubscriberCount;
                    System.out.println("Received subscriber count: " + brokerSubscriberCount);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid subscriber count received from broker: " + response);
                }
    
            } catch (IOException | InterruptedException e) {
                System.err.println("Error communicating with broker: " + e.getMessage());
            }
        }
    
        return totalSubscriberCount;  // Return the total count of subscribers across all brokers
    }
    
    public int getTotalPublisherCount() {
        int totalPublisherCount = getLocalPublisherCount();  // Start with the local publisher count
        System.out.println(brokerBrokerHandlers.size() + " brokers connected.");
    
        for (BrokerHandler brokerHandler : brokerBrokerHandlers) {
            Socket brokerSocket = brokerHandler.getSocket();  // Get the broker's socket
    
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                
                // Send a request to the broker to get its local publisher count
                out.println("get_local_publisher_count");
                out.flush();
    
                // Poll the response map in BrokerHandler to get the result
                String response;
                while ((response = brokerHandler.getResponse("get_local_publisher_count")) == null) {
                    Thread.sleep(50); // Short delay before re-checking
                }
    
                // Parse the response as an integer
                try {
                    int brokerPublisherCount = Integer.parseInt(response);
                    totalPublisherCount += brokerPublisherCount;
                    System.out.println("Received publisher count: " + brokerPublisherCount);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid publisher count received from broker: " + response);
                }
    
            } catch (IOException | InterruptedException e) {
                System.err.println("Error communicating with broker: " + e.getMessage());
            }
        }
    
        return totalPublisherCount;  // Return the total count of publishers across all brokers
    }
    

    public int getLocalPublisherCount() {
        return this.pubClientHandlers.size();
    }

    public void removePublisherClientHandler(String username) {
        for (ClientHandler clientHandler : pubClientHandlers) {
            if (clientHandler.getUserName().equals(username)) {
                // Remove the publisher's ClientHandler from the list
                pubClientHandlers.remove(clientHandler);
                System.out.println("Removed publisher ClientHandler for: " + username);
                break; // Exit loop after finding and removing the publisher
            }
        }
    }

    public void removeSubscriberClientHandler(String username) {
        for (ClientHandler clientHandler : subClientHandlers) {
            if (clientHandler.getUserName().equals(username)) {
                // Remove the publisher's ClientHandler from the list
                subClientHandlers.remove(clientHandler);
                System.out.println("Removed subscriber ClientHandler for: " + username);
                break; // Exit loop after finding and removing the publisher
            }
        }
    }
    public boolean isSubscribed(String topicId, String username) {
        ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);
        return subscribers != null && subscribers.containsKey(username);
    }
    
}
