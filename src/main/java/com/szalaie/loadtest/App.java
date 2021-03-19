package com.szalaie.loadtest;

import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {

    public static final String GETTING_RESULT_MSG = "Getting results%n";

    public static void main(String[] args) {
        String broker = "tcp://0.0.0.0:1883";
        String subscriberClientId = "test02";
        String clientIdBase = "device";
        String password = "passw";
        String topic = "/device/test_type";
        int publisherClientNumber = 50;
        String publisherClientPassword = "passw";
        int delayBetweenMessagesInMillisec = 15;
        int messageNumber = 30000;
        int qos = 2;
        int awaitTerminationInSecs = 240;
        ExecutorServiceHandler executorServiceHandler;

        try {
            final ExecutorService executorService = Executors.newScheduledThreadPool(messageNumber);
            executorServiceHandler = new ExecutorServiceHandler(executorService);

            List<Client> publisherClientList = ClientUtils.createClients(publisherClientNumber, broker, clientIdBase,
                    publisherClientPassword);
            Client subscriber = new Client(broker, subscriberClientId, password);
            List<Client> subscriberClientList = new LinkedList<>();

            subscriberClientList.add(subscriber);

            ClientUtils.connect(subscriberClientList);
            ClientUtils.connect(publisherClientList);

            ClientUtils.subscribe(subscriberClientList, topic, qos);

            List<Runnable> runnableList = ClientUtils.createRunnablesToPublishMessage(publisherClientList, topic, qos,
                    messageNumber);

            executorServiceHandler.scheduleCommands(runnableList, delayBetweenMessagesInMillisec);
            executorServiceHandler.shutdownExecutorService(awaitTerminationInSecs);

            ClientUtils.disconnect(subscriberClientList);
            ClientUtils.disconnect(publisherClientList);

            System.out.printf(GETTING_RESULT_MSG);
            Utils.writeResultToFile(subscriberClientList, publisherClientList, qos, messageNumber);
        } catch (MqttException | InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
