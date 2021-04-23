package com.szalaie.loadtest;

import org.eclipse.paho.client.mqttv3.MqttException;

public class PublishMessageThread<T> implements Runnable {
    T client;
    String topic;
    int qos;

    public PublishMessageThread(T client, String topic, int qos) {
        this.client = client;
        this.topic = topic;
        this.qos = qos;
    }

    @Override
    public void run() {
        try {
            if (client instanceof Client) {
                ((Client) client).publishWithTimePayload(topic, qos, false);
            } else if (client instanceof AsyncClient) {
                ((AsyncClient) client).publishWithTimePayload(topic, qos, false);
            }
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}