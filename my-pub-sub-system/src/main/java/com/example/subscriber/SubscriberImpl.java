package com.example.subscriber;

import java.io.PrintWriter;

public class SubscriberImpl implements Subscriber {
    private final String subscriberId;
    private final PrintWriter out;

    public SubscriberImpl(String subscriberId, PrintWriter out) {
        this.subscriberId = subscriberId;
        this.out = out;
    }

    @Override
    public void receiveMessage(String topicName, String message) {
        out.println("Received message on topic " + topicName + ": " + message);
    }

    @Override
    public String getSubscriberId() {
        return subscriberId;
    }

    @Override
    public void subscribe(String topicName) {
        out.println("Subscribed to: " + topicName);
    }

    @Override
    public void unsubscribe(String topicName) {
        out.println("Unsubscribed from: " + topicName);
    }
}
