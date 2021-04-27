package com.szalaie.loadtest;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class Client extends AbstractClient {

    private MqttClient client;
    private final AtomicInteger messageCounter;

    public Client(String broker, String clientId, String password, String clientType) throws MqttException {
        super(clientId, password, clientType);
        this.client = new MqttClient(broker, clientId, new MemoryPersistence());

        messageCounter = new AtomicInteger(0);

        setCallbacks();
    }

    public void setCallbacks() {
        this.client.setCallback(new MqttCallback() {

            // Called when the client lost the connection to the broker
            @Override
            public void connectionLost(Throwable cause) {
                System.out.printf(CONNECTION_LOST_MSG, clientId, cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                System.out.printf(MSG_ARRIVED_MSG, clientId, message.getId());
                numberOfArrivedMessages.getAndIncrement();
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
                if (token.getException() != null) {
                    System.out.printf(ERROR_IN_DELIVERY_MSG, token.getException().getStackTrace().toString());
                } else {
                    numberOfSuccessfullyDeliveredMessages.getAndIncrement();
                    deliveryCompleteTimeByMessageId.put(token.getMessageId(), Instant.now());
                }
            }
        });
    }

    public void connect() throws MqttException {
        System.out.printf(CONNECT_CLIENT_MSG, this.clientId);
        this.client.connect(this.options);
    }

    public void disconnect() throws MqttException {
        System.out.printf(DISCONNECT_CLIENT_MSG, this.clientId);
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
        Instant currentTime = Instant.now();
        byte[] payload = currentTime.toString().getBytes(StandardCharsets.UTF_8);
        MqttMessage message = new MqttMessage(payload);
        message.setId(messageCounter.getAndIncrement());
        message.setQos(qos);
        message.setRetained(retained);
        sendingMessageTimeByMessageId.put(message.getId(), currentTime);
        this.client.publish(topic, message);
    }

    public void publish(String topic, MqttMessage mqttMessage) throws MqttException {
        this.client.publish(topic, mqttMessage);
    }
}
