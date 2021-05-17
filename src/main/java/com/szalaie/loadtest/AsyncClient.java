package com.szalaie.loadtest;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

public class AsyncClient extends AbstractClient {

    private MqttAsyncClient client;

    public AsyncClient(String broker, String clientId, String password, String clientType) throws MqttException {
        super(clientId, password, clientType);
        client = new MqttAsyncClient(broker, clientId, new MqttDefaultFilePersistence("../tmp"));

        setCallbacks();
    }

    void setCallbacks() {
        this.client.setCallback(new MqttCallback() {

            // Called when the client lost the connection to the broker
            @Override
            public void connectionLost(Throwable cause) {
                System.out.printf(CONNECTION_LOST_MSG, getClientId(), cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                // System.out.printf(MSG_ARRIVED_MSG, clientId, message.getId());
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
                    numberOfSuccessfullyDeliveredMessages.incrementAndGet();
                    deliveryCompleteTimeByMessageId.put(token.getMessageId(), Instant.now());
                }
            }
        });
    }

    public IMqttToken connect() throws MqttException {
        System.out.printf(CONNECT_CLIENT_MSG, this.getClientId());
        return this.client.connect(this.getOptions());
    }

    public IMqttToken disconnect() throws MqttException {
        System.out.printf(DISCONNECT_CLIENT_MSG, this.getClientId());
        return this.client.disconnect();
    }

    public void waitForCompletion(IMqttToken token) throws MqttException {
        System.out.printf(WAIT_FOR_CONNECTION_COMPLETION, this.getClientId());
        token.waitForCompletion();
    }

    public IMqttToken subscribe(String topic, int qos) throws MqttException {
        return this.client.subscribe(topic, qos);
    }

    public IMqttToken unsubscribe(String topic) throws MqttException {
        return this.client.unsubscribe(topic);
    }

    public void publish(String topic, byte[] payload, int qos, boolean retained) throws MqttException {
        this.client.publish(topic, payload, qos, retained);
    }

    public IMqttDeliveryToken publishWithTimePayload(String topic, int qos, boolean retained) throws MqttException {
        Instant currentTime = Instant.now();
        byte[] payload = currentTime.toString().getBytes(StandardCharsets.UTF_8);
        IMqttDeliveryToken deliveryToken = this.client.publish(topic, payload, qos, retained);
        sendingMessageTimeByMessageId.put(deliveryToken.getMessageId(), currentTime);
        return deliveryToken;
    }
}