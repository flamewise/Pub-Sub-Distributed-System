package com.example.broker;

import com.example.subscriber.Subscriber;
import com.example.directory.DirectoryServiceClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Broker {
    private final String ownBrokerAddress;  // Store the broker's own address
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Subscriber>> topicSubscribers; // topicId -> (username -> Subscriber)
    private final ConcurrentHashMap<String, String> topicNames; // topicId -> topicName
    private final ConcurrentHashMap<String, String> topicPublishers;  // topicId -> publisherUsername
    private final ConcurrentHashMap<String, String> subscriberUsernames;  // Map username to topicId
    private final Set<String> connectedBrokerAddresses = ConcurrentHashMap.newKeySet(); // Stores connected brokers
    private final CopyOnWriteArrayList<Socket> connectedBrokers;
    private final ExecutorService connectionPool;
    private final DirectoryServiceClient directoryServiceClient;
    private final ServerSocket serverSocket;

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
            connectionPool.submit(new BrokerHandler(clientSocket, this, username));
        } else if ("publisher".equals(connectionType) || "subscriber".equals(connectionType)) {
            // Handle publisher or subscriber connection
            connectionPool.submit(new ClientHandler(clientSocket, this, username, connectionType));
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
            ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);
    
            if (subscribers != null && !subscribers.isEmpty()) {
                // Deliver the message to local subscribers
                for (Subscriber subscriber : subscribers.values()) {
                    subscriber.receiveMessage(topicId, message);
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
    

    public void addSubscriber(String topicId, Subscriber subscriber, String username, boolean synchronizedRequired) {
        // Check if the topic already has a subscriber list; if not, create one
        ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.computeIfAbsent(topicId, k -> new ConcurrentHashMap<>());
        
        // Add the subscriber if they are not already in the list
        if (!subscribers.containsKey(username)) {
            subscribers.put(username, subscriber);
            subscriberUsernames.put(username, topicId);
            
            if (synchronizedRequired) {
                // Synchronize the subscription across all brokers immediately
                synchronizeSubscription(topicId, username);
            }
        }
    }
    
    public void unsubscribe(String topicId, String username) {
        ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);
        if (subscribers != null && subscribers.containsKey(username)) {
            subscribers.remove(username);
            subscriberUsernames.remove(username);
        }
    }

    // Method to list all topics to a subscriber
    public void listAllTopics(PrintWriter out) {
        if (topicNames.isEmpty()) {
            out.println("No topics available.");
        } else {
            out.println("Available topics:");
            for (String topicId : topicNames.keySet()) {
                String topicName = topicNames.get(topicId);
                //System.out.println("Topic ID: " + topicId + ", Name: " + topicName);
                out.println("Topic ID: " + topicId + ", Name: " + topicName);
            }
        }
        out.println("END");  // Indicate the end of the topic list
    }

    public void listSubscriptions(PrintWriter out, String subscriberId) {
        for (String topicId : topicSubscribers.keySet()) {
            ConcurrentHashMap<String, Subscriber> subscribers = topicSubscribers.get(topicId);
            if (subscribers.containsKey(subscriberId)) {
                out.println("Subscribed to: " + topicId + " (" + topicNames.get(topicId) + ")");
            }
        }
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

    public void synchronizeSubscription(String topicId, String subscriberId) {
        // Update the list of connected brokers before synchronization
        this.updateConnectedBrokers();
        
        // Synchronize subscription with all connected brokers
        for (Socket brokerSocket : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                // Send the synchronization message to the connected broker
                out.println("synchronize_sub " + topicId + " " + subscriberId);
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
            connectionPool.submit(new BrokerHandler(brokerSocket, this, brokerIP + ":" + brokerPort));
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
}
