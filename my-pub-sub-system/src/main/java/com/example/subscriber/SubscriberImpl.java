package com.example.subscriber;

import com.example.broker.Broker;

public class SubscriberImpl implements Subscriber {
    private final Broker broker;
    private final String id;

    // Constructor that takes a Broker and a subscriber ID
    public SubscriberImpl(Broker broker, String id) {
        this.broker = broker;
        this.id = id;
    }

    @Override
    public void subscribe(String topicName) {
        broker.addSubscriber(topicName, this);
        System.out.println("Subscribed to topic: " + topicName);
    }

    @Override
    public void receiveMessage(String topicName, String message) {
        System.out.println("Received message on topic " + topicName + ": " + message);
    }

    @Override
    public void unsubscribe(String topicName) {
        broker.unsubscribe(topicName, id);
        System.out.println("Unsubscribed from topic: " + topicName);
    }

    @Override
    public String getSubscriberId() {
        return id;
    }
}
