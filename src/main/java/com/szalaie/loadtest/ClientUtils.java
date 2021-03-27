package com.szalaie.loadtest;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.eclipse.paho.client.mqttv3.MqttException;

public class ClientUtils {

    final static String EMPTY_STR = "";

    static List<Client> createClients(int clientNumber, String broker, String clientIdBase, String type,
            int firstClientIdNumber, String clientPassword) throws MqttException {
        List<Client> clientList = new LinkedList<>();

        for (int i = firstClientIdNumber; i < firstClientIdNumber + clientNumber; i++) {
            String clientId = clientIdBase + i;
            Client client = new Client(broker, clientId, clientPassword, type);
            clientList.add(client);
        }

        return clientList;
    }

    static List<Runnable> createRunnablesToPublishMessage(List<Client> clientList, String topic, int qos,
            int messageNumber) {
        int messageCounter = 0;
        List<Runnable> runnableList = new ArrayList<>();
        Iterator<Client> clientListIterator = clientList.iterator();
        while (clientListIterator.hasNext()) {
            Client client = clientListIterator.next();
            String topicToPublish = topic == EMPTY_STR ? client.getDefaultTopic() : topic;
            runnableList.add(new PublishMessageThread(client, topicToPublish, qos));
            messageCounter++;

            if (messageCounter >= messageNumber)
                break;

            if (!clientListIterator.hasNext())
                clientListIterator = clientList.iterator();
        }
        return runnableList;
    }

    static void connect(List<Client> clientList) throws MqttException {
        for (Client client : clientList) {
            client.connect();
        }
    }

    static void disconnect(List<Client> clientList) throws MqttException {
        for (Client client : clientList) {
            client.disconnect();
        }
    }

    static void subscribe(List<Client> clientList, String topic, int qos) throws MqttException {
        for (Client client : clientList) {
            client.subscribe(topic, qos);
        }
    }
}