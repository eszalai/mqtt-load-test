package com.szalaie.loadtest;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.eclipse.paho.client.mqttv3.MqttException;

public class LoadTester {

    private static final String CLIENT_CONN_STARTED_MSG = "Client connection started at: %s%n";
    private static final String CLIENT_CONN_ENDED_MSG = "Client connection ended at: %s%n";
    private static final String CLIENT_DISCONN_STARTED_MSG = "Client disconnection started at: %s%n";
    private static final String CLIENT_DISCONN_ENDED_MSG = "Client disconnection ended at: %s%n";
    private static final String CLIENT_SUB_STARTED_MSG = "Client subscribing started at: %s%n";
    private static final String CLIENT_SUB_ENDED_MSG = "Client subscribing ended at: %s%n";
    private static final String GETTING_RESULTS_MSG = "Getting results%n";
    private static final String NO_PUBLISHER_MSG = "No publisher%n";
    private static final String WAITING_FOR_ALL_MESSAGES_TO_ARRIVE_MSG = "Waiting for all messages to arrive%n";
    private static final String ALL_MESSAGES_ARRIVED_MSG = "All messages have arrived: %d %s%n";
    private static final String ARRIVED_MESSAGES_MSG = "Arrived messages: %d%n";

    ExecutorServiceHandler executorServiceHandler;

    public LoadTester(ScheduledThreadPoolExecutor executorService) {
        executorServiceHandler = new ExecutorServiceHandler(executorService);
    }

    private <T> void waitingForAllMessagesToArrive(List<T> subscriberClientList, int messageNumber)
            throws InterruptedException {
        System.out.printf(WAITING_FOR_ALL_MESSAGES_TO_ARRIVE_MSG);
        while (true) {
            Thread.sleep(10000);
            int arrivedMessages = 0;
            for (T subscriber : subscriberClientList) {
                if (subscriber instanceof Client) {
                    arrivedMessages += ((Client) subscriber).getNumberOfArrivedMessages();
                } else if (subscriber instanceof AsyncClient) {
                    arrivedMessages += ((AsyncClient) subscriber).getNumberOfArrivedMessages();
                }
            }
            System.out.printf(ARRIVED_MESSAGES_MSG, arrivedMessages);
            if (arrivedMessages >= messageNumber) {
                System.out.printf(ALL_MESSAGES_ARRIVED_MSG, arrivedMessages, Instant.now().toString());
                break;
            }
        }
    }

    void runTest(LoadTesterParams params) throws MqttException, InterruptedException, IOException {
        List<AsyncClient> publishertList = ClientUtils.createAsyncClients(params.getPublisherNumber(),
                params.getBrokerUrl(), params.getClientIdBase(), params.getClientType(),
                params.getFirstClientIdNumber(), params.getClientPassword());
        List<AsyncClient> subscriberList = ClientUtils.createAsyncClients(params.getSubscriberNumber(),
                params.getBrokerUrl(), params.getClientIdBase(), params.getClientType(),
                params.getPublisherNumber() + params.getFirstClientIdNumber(), params.getClientPassword());

        System.out.printf(CLIENT_CONN_STARTED_MSG, Instant.now().toString());
        ClientUtils.connect(subscriberList);
        ClientUtils.connect(publishertList);
        System.out.printf(CLIENT_CONN_ENDED_MSG, Instant.now().toString());

        System.out.printf(CLIENT_SUB_STARTED_MSG, Instant.now().toString());
        ClientUtils.subscribe(subscriberList, params.getTopicToSubscribe(), params.getQos());
        System.out.printf(CLIENT_SUB_ENDED_MSG, Instant.now().toString());

        this.executorServiceHandler.setMessageNumber(params.getMessageNumber());

        if (publishertList.size() > 0) {
            int schedulerNumber = 1 + params.getLoadIncreasingNumber();
            int initDelayInMillis = 0;
            Instant sendingTime = Instant.now();

            for (int i = 0; i < schedulerNumber; i++) {
                executorServiceHandler.publishMessagesAtFixedRate(publishertList, params.getQos(),
                        params.getTopicToPublish(), initDelayInMillis, params.getDelayBetweenMessagesInMillisec());
                initDelayInMillis += params.getLoadIncreasingRate();
            }

            executorServiceHandler.waitingForAllMessagesToBeSent();
            executorServiceHandler.shutdownExecutorService(params.getConnectionTerminationInSecs());

            Instant deliveryCompleteTime = Instant.now();
            Duration timeElapsed = Duration.between(sendingTime, deliveryCompleteTime);

            System.out.printf(CLIENT_DISCONN_STARTED_MSG, Instant.now().toString());
            ClientUtils.disconnect(publishertList);
            ClientUtils.disconnect(subscriberList);
            System.out.printf(CLIENT_DISCONN_ENDED_MSG, Instant.now().toString());

            System.out.printf(GETTING_RESULTS_MSG);
            Utils.writeDelayValuesToFile(publishertList);
            Utils.writeResultsToFile(subscriberList, publishertList, params.getQos(), params.getMessageNumber(),
                    executorServiceHandler.getNumberOfSentMessages(), timeElapsed);
        } else {
            System.out.printf(NO_PUBLISHER_MSG);
            waitingForAllMessagesToArrive(subscriberList, params.getMessageNumber());
        }
    }
}