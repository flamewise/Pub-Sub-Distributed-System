package com.example;

import com.example.broker.Broker;
import com.example.broker.BrokerImpl;
import com.example.publisher.Publisher;
import com.example.publisher.PublisherImpl;
import com.example.subscriber.Subscriber;
import com.example.subscriber.SubscriberImpl;

public class App {
    public static void main(String[] args) {
        try {
            // Start broker on port 12345
            Broker broker = new BrokerImpl(12345);
            new Thread(() -> broker.start()).start();  // Run broker in a separate thread

            // Create publisher and subscribers
            Publisher publisher = new PublisherImpl("localhost", 12345);
            Subscriber subscriber1 = new SubscriberImpl(broker, "subscriber1");  // Update constructor usage
            Subscriber subscriber2 = new SubscriberImpl(broker, "subscriber2");  // Update constructor usage

            // Publisher creates a topic and publishes a message
            publisher.createTopic("news");
            publisher.publishMessage("news", "Breaking news: Pub-sub system implemented!");

            // Subscribers subscribe to the topic
            subscriber1.subscribe("news");
            subscriber2.subscribe("news");

            // Publisher publishes another message
            publisher.publishMessage("news", "Another update: Pub-sub system is working!");

            // Subscriber1 unsubscribes
            subscriber1.unsubscribe("news");

            // Publisher publishes a final message
            publisher.publishMessage("news", "Final update: Subscriber1 won't receive this.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
