package com.szalaie.loadtest;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

public abstract class AbstractClient {

    static final String CONNECTION_LOST_MSG = "Connection lost - clientId: %s cause: %s%n";
    static final String CONNECT_CLIENT_MSG = "Connect client: %s%n";
    static final String MSG_ARRIVED_MSG = "Message arrived: clientId: %s, messageId: %s%n";
    static final String WAIT_FOR_CONNECTION_COMPLETION = "Wait for connection completion: %s%n";
    static final String DISCONNECT_CLIENT_MSG = "Disconnect client: %s%n";
    static final String NO_MESSAGE_RECEIVED_MSG = "No message received%n";
    static final String ERROR_IN_DELIVERY_MSG = "Error in delivery: %s%n";
    static final String CLIENT = "Client: %S";
    static final String DEFAULT_TOPIC_STR = "/device/%s/%s";
    static final int MAX_INFLIGHT = 60000;

    String clientId;
    String defaultTopic;
    String clientType;
    MqttConnectOptions options;
    final AtomicInteger numberOfSuccessfullyDeliveredMessages;
    final AtomicInteger numberOfArrivedMessages;
    Map<Integer, Instant> sendingMessageTimeByMessageId;
    Map<Integer, Instant> deliveryCompleteTimeByMessageId;

    public AbstractClient(String clientId, String password, String clientType) {
        this.clientId = clientId;
        this.clientType = clientType;
        this.defaultTopic = String.format(DEFAULT_TOPIC_STR, this.clientType, this.clientId);

        options = new MqttConnectOptions();
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        options.setMaxInflight(MAX_INFLIGHT);

        options.setCleanSession(true);
        options.setUserName(clientId);
        options.setPassword(password.toCharArray());

        numberOfSuccessfullyDeliveredMessages = new AtomicInteger(0);
        numberOfArrivedMessages = new AtomicInteger(0);
        sendingMessageTimeByMessageId = new HashMap<>();
        deliveryCompleteTimeByMessageId = new HashMap<>();
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

    public int getSuccessfullySentMessagesNumber() {
        return this.numberOfSuccessfullyDeliveredMessages.get();
    }

    public int getNumberOfArrivedMessages() {
        return this.numberOfArrivedMessages.get();
    }

    public List<Long> getDelays() {
        List<Long> delays = new LinkedList<>();

        for (int messageId : sendingMessageTimeByMessageId.keySet()) {
            Instant sendingTime = sendingMessageTimeByMessageId.get(messageId);
            Instant deliveryCompleteTime = deliveryCompleteTimeByMessageId.get(messageId);
            long timeElapsedInMillis = -1;
            if (deliveryCompleteTime != null) {
                Duration timeElapsed = Duration.between(sendingTime, deliveryCompleteTime);
                timeElapsedInMillis = timeElapsed.toMillis();
            }

            delays.add(timeElapsedInMillis);
        }

        return delays;
    }

    @Override
    public String toString() {
        return String.format(CLIENT, this.clientId);
    }
}
