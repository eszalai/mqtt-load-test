package com.szalaie.loadtest;

import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class App {

    public static final String GETTING_RESULT_MSG = "Getting results%n";
    public static final String END_OF_TESTING_MSG = "End of testing%n";

    public static void main(String[] args) {
        String broker = "tcp://0.0.0.0:1883";
        String clientIdBase = "device";
        String topic = "/device/test_type";
        int publisherClientNumber = 50;
        int subscriberClientNumber = 50;
        String publisherClientPassword = "passw";
        int delayBetweenMessagesInMillisec = 15;
        int messageNumber = 30000;
        int qos = 2;
        int awaitTerminationInSecs = 240;
        ExecutorServiceHandler executorServiceHandler;

        try {
            final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(messageNumber);
            executorServiceHandler = new ExecutorServiceHandler(executorService);

            List<Client> publisherClientList = ClientUtils.createClients(publisherClientNumber, broker, clientIdBase, 1,
                    publisherClientPassword);
            List<Client> subscriberClientList = ClientUtils.createClients(subscriberClientNumber, broker, clientIdBase,
                    51, publisherClientPassword);

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
            Utils.writeLatenciesToFile(subscriberClientList);
            Utils.writeResultToFile(subscriberClientList, publisherClientList, qos, messageNumber);
            System.out.printf(END_OF_TESTING_MSG);
        } catch (MqttException | InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
