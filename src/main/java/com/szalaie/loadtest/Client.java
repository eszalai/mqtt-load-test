package com.szalaie.loadtest;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class Client {

    static final String CONNECTION_LOST_MSG = "Connection lost - clientId: %s cause: %s%n";
    static final String CONNECT_CLIENT_MSG = "Connect client: %s%n";
    static final String WAIT_FOR_CONNECTION_COMPLETION = "Wait for connection completion: %s%n";
    static final String DISCONNECT_CLIENT_MSG = "Disconnect client: %s%n";
    static final String NO_MESSAGE_RECEIVED_MSG = "No message received%n";
    static final String ERROR_IN_DELIVERY_MSG = "Error in delivery: %s%n";
    static final String CLIENT = "Client: %S";
    static final String DEFAULT_TOPIC_STR = "/device/%s/%s";
    static final int MAX_INFLIGHT = 60000;

    MqttClient client;
    String clientId;
    String defaultTopic;
    String clientType;
    MqttConnectOptions options;
    final AtomicInteger messageCounter;
    final AtomicInteger numberOfSuccessfullyDeliveredMessages;
    Map<Integer, Instant> sendingMessageTimeByMessageId;
    Map<Integer, Instant> deliveryCompleteTimeByMessageId;

    public Client(String broker, String clientId, String password, String clientType) throws MqttException {
        this.clientId = clientId;
        this.clientType = clientType;
        this.defaultTopic = String.format(DEFAULT_TOPIC_STR, this.clientType, this.clientId);
        client = new MqttClient(broker, clientId, new MemoryPersistence());

        options = new MqttConnectOptions();
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        options.setMaxInflight(MAX_INFLIGHT);

        options.setCleanSession(true);
        options.setUserName(clientId);
        options.setPassword(password.toCharArray());

        numberOfSuccessfullyDeliveredMessages = new AtomicInteger(0);
        messageCounter = new AtomicInteger(0);
        sendingMessageTimeByMessageId = new HashMap<>();
        deliveryCompleteTimeByMessageId = new HashMap<>();

        setCallbacks();
    }

    public String getClientId() {
        return this.clientId;
    }

    public String getDefaultTopic() {
        return this.defaultTopic;
    }

    public Map<Integer, Instant> getDeliveryCompleteTimeByMessageId() {
        return this.deliveryCompleteTimeByMessageId;
    }

    public Map<Integer, Instant> getSendingMessageTimeByMessageId() {
        return this.sendingMessageTimeByMessageId;
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

    public int getSuccessfullySentMessagesNumber() {
        return this.numberOfSuccessfullyDeliveredMessages.get();
    }

    public List<Long> getDelays() {
        List<Long> delays = new LinkedList<>();

        for (int messageId : sendingMessageTimeByMessageId.keySet()) {
            Instant sendingTime = sendingMessageTimeByMessageId.get(messageId);
            Instant deliveryCompleteTime = deliveryCompleteTimeByMessageId.get(messageId);
            Duration timeElapsed = Duration.between(sendingTime, deliveryCompleteTime);

            delays.add(timeElapsed.toMillis());
        }

        return delays;
    }

    @Override
    public String toString() {
        return String.format(CLIENT, this.clientId);
    }
}
