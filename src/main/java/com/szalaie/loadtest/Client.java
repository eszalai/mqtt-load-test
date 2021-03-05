package com.szalaie.loadtest;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

public class Client {
    MqttClient client;
    String clientId;
    MqttConnectOptions options;
    final AtomicInteger count;
    Map<Long, byte[]> payloads;

    public Client(String broker, String clientId, String password, MemoryPersistence persistence) throws MqttException {
        this.clientId = clientId;
        client = new MqttClient(broker, clientId, persistence);
        options = new MqttConnectOptions();

        options.setCleanSession(true);
        options.setUserName(clientId);
        options.setPassword(password.toCharArray());

        count = new AtomicInteger(0);
        payloads = new HashMap<>();

        setCallbacks();
    }

    public void setCallbacks() {
        this.client.setCallback(new MqttCallback() {

            // Called when the client lost the connection to the broker
            @Override
            public void connectionLost(Throwable cause) {
                System.out.println("Connection lost - clientId: " + clientId + " cause: " + cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                payloads.put(ZonedDateTime.now().toInstant().toEpochMilli(), message.getPayload());
            }

            // Called when delivery for a message has been completed, and all
            // acknowledgments have been received.
            // For QoS 0 messages it is called once the message has been handed to the
            // network for delivery.
            // For QoS 1 it is called when PUBACK is received and for QoS 2 when PUBCOMP is
            // received.
            // The token will be the same token as that returned when the message was
            // published.
            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                count.getAndIncrement();
            }
        });
    }

    public void connect() throws MqttException {
        System.out.println("Connecting client: " + this.clientId);
        this.client.connect(this.options);
    }

    public void disconnect() throws MqttException {
        System.out.println("Disconnecting client: " + this.clientId);
        this.client.disconnect();
    }

    public void subscribe(String topic, int qos) throws MqttException {
        this.client.subscribe(topic, qos);
    }

    public void unsubscribe(String topic) throws MqttException {
        this.client.unsubscribe(topic);
    }

    public void publish(String topic, byte[] payload, int qos, boolean retained) throws MqttException {
        this.client.publish(topic, payload, qos, retained);
    }

    public void publishWithTimePayload(String topic, int qos, boolean retained) throws MqttException {
        long timeMilli = ZonedDateTime.now().toInstant().toEpochMilli();
        byte[] payload = Utils.longToBytes(timeMilli);
        this.client.publish(topic, payload, qos, retained);
    }

    public void publish(String topic, MqttMessage mqttMessage) throws MqttException {
        this.client.publish(topic, mqttMessage);
    }

    public int getSuccessfullySentMessagesNumber() {
        return this.count.get();
    }

    public List<Long> getLatencies() {
        List<Long> latencies = new LinkedList<>();

        if (payloads != null && !payloads.isEmpty()) {
            for (long receivingTime : payloads.keySet()) {
                long latency = Utils.calculateLatency(receivingTime, payloads.get(receivingTime));
                latencies.add(latency);
            }
        } else {
            System.out.println("Have not received any messages yet");
        }
        return latencies;
    }

    @Override
    public String toString() {
        return String.format("Client: %S", this.clientId);
    }
}
