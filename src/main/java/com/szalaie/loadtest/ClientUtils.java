package com.szalaie.loadtest;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.eclipse.paho.client.mqttv3.MqttException;

public class ClientUtils {

    static List<Client> createClients(int clientNumber, String broker, String clientIdBase, String clientPassword)
            throws MqttException {
        List<Client> clientList = new LinkedList<>();

        for (int i = 1; i <= clientNumber; i++) {
            String clientId = clientIdBase + i;
            Client client = new Client(broker, clientId, clientPassword);
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
            runnableList.add(new PublishMessageThread(client, topic, qos));
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