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

    static List<AsyncClient> createAsyncClients(int clientNumber, String broker, String clientIdBase, String type,
            int firstClientIdNumber, String clientPassword) throws MqttException {
        List<AsyncClient> clientList = new LinkedList<>();

        for (int i = firstClientIdNumber; i < firstClientIdNumber + clientNumber; i++) {
            String clientId = clientIdBase + i;
            AsyncClient client = new AsyncClient(broker, clientId, clientPassword, type);
            clientList.add(client);
        }

        return clientList;
    }

    static <T> List<Runnable> createRunnablesToPublishMessage(List<T> clientList, String topic, int qos,
            int messageNumber) {
        int messageCounter = 0;
        List<Runnable> runnableList = new ArrayList<>();
        Iterator<T> clientListIterator = clientList.iterator();
        while (clientListIterator.hasNext()) {
            T client = clientListIterator.next();
            String defaultTopic = EMPTY_STR;
            if (client instanceof Client) {
                defaultTopic = ((Client) client).getDefaultTopic();
            } else if (client instanceof AsyncClient) {
                defaultTopic = ((AsyncClient) client).getDefaultTopic();
            }
            String topicToPublish = topic == EMPTY_STR ? defaultTopic : topic;
            runnableList.add(new PublishMessageThread<T>(client, topicToPublish, qos));
            messageCounter++;

            if (messageCounter >= messageNumber)
                break;

            if (!clientListIterator.hasNext())
                clientListIterator = clientList.iterator();
        }
        return runnableList;
    }

    static <T> void connect(List<T> clientList) throws MqttException {
        for (T client : clientList) {
            if (client instanceof Client) {
                ((Client) client).connect();
            } else if (client instanceof AsyncClient) {
                ((AsyncClient) client).connect().waitForCompletion();
            }
        }
    }

    static <T> void disconnect(List<T> clientList) {
        for (T client : clientList) {
            try {
                if (client instanceof Client) {
                    ((Client) client).disconnect();
                } else if (client instanceof AsyncClient) {
                    ((AsyncClient) client).disconnect();
                }
            } catch (MqttException e) {
                System.out.println(e.getCause() + " " + e.toString());
            }
        }
    }

    static void waitForCompletion(List<AsyncClient> clientList) throws MqttException {
        for (AsyncClient client : clientList) {
            client.waitForCompletion();
        }
    }

    static <T> void subscribe(List<T> clientList, String topic, int qos) throws MqttException {
        for (T client : clientList) {
            if (client instanceof Client) {
                ((Client) client).subscribe(topic, qos);
            } else if (client instanceof AsyncClient) {
                ((AsyncClient) client).subscribe(topic, qos).waitForCompletion();
            }
        }
    }
}