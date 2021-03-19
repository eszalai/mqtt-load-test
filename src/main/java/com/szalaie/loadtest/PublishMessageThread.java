package com.szalaie.loadtest;

import org.eclipse.paho.client.mqttv3.MqttException;

public class PublishMessageThread implements Runnable {
    Client client;
    String topic;
    int qos;

    public PublishMessageThread(Client client, String topic, int qos) {
        this.client = client;
        this.topic = topic;
        this.qos = qos;
    }

    @Override
    public void run() {
        try {
            client.publishWithTimePayload(topic, qos, false);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}