package com.szalaie.loadtest;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.eclipse.paho.client.mqttv3.MqttException;

public class LoadTester {

        public static final String CLIENT_CONN_TESTING_MSG = "Client connection testing%n";
        public static final String CLIENT_CONN_TESTING_START_MSG = "Client connection testing START: %s%n";
        public static final String CLIENT_CONN_TESTING_END_MSG = "Client connection testing END: %s%n";
        public static final String CLIENT_CONN_STARTED_MSG = "Client connection started at: %s%n";
        public static final String CLIENT_CONN_ENDED_MSG = "Client connection ended at: %s%n";
        public static final String CLIENT_DISCONN_STARTED_MSG = "Client disconnection started at: %s%n";
        public static final String CLIENT_DISCONN_ENDED_MSG = "Client disconnection ended at: %s%n";
        public static final String CLIENT_SUB_STARTED_MSG = "Client subscribing started at: %s%n";
        public static final String CLIENT_SUB_ENDED_MSG = "Client subscribing ended at: %s%n";
        public static final String GETTING_RESULTS_MSG = "Getting results%n";
        public static final String END_OF_TESTING_MSG = "End of testing%n";
        public static final String NO_PUBLISHER_MSG = "No publisher%n";
        public static final String WAITING_FOR_ALL_MESSAGES_TO_ARRIVE_MSG = "Waiting for all messages to arrive%n";
        public static final String ALL_MESSAGES_ARRIVED_MSG = "All messages have arrived: %d %s%n";
        static final int ONE_MINUTE_IN_MILLIS = 60000;

        ExecutorServiceHandler executorServiceHandler;

        public LoadTester(ScheduledThreadPoolExecutor executorService) {
                executorServiceHandler = new ExecutorServiceHandler(executorService);
        }

        void clientConnectionTesting(String broker, String clientIdBase, int firstClientIdNumber, String clientType,
                        String clientPassword, int clientNumber) throws MqttException {
                System.out.printf(CLIENT_CONN_TESTING_MSG);
                List<Client> clients = ClientUtils.createClients(clientNumber, broker, clientIdBase, clientType,
                                firstClientIdNumber, clientPassword);

                System.out.printf(CLIENT_CONN_STARTED_MSG, Instant.now().toString());
                ClientUtils.connect(clients);

                System.out.printf(CLIENT_CONN_ENDED_MSG, Instant.now().toString());
                ClientUtils.disconnect(clients);
        }

        <T> void waitForAllMessagesArrive(List<T> subscriberClientList, int messageNumber) {
                System.out.printf(WAITING_FOR_ALL_MESSAGES_TO_ARRIVE_MSG);
                while (true) {
                        int arrivedMessages = 0;
                        for (T subscriber : subscriberClientList) {
                                if (subscriber instanceof Client) {
                                        arrivedMessages += ((Client) subscriber).getNumberOfArrivedMessages();
                                } else if (subscriber instanceof AsyncClient) {
                                        arrivedMessages += ((AsyncClient) subscriber).getNumberOfArrivedMessages();
                                }
                        }
                        if (arrivedMessages >= messageNumber) {
                                System.out.printf(ALL_MESSAGES_ARRIVED_MSG, arrivedMessages, Instant.now().toString());
                                break;
                        }
                }
        }

        void publishMessagesAsynchWithRate(String broker, String clientIdBase, int firstClientIdNumber,
                        String clientType, String clientPassword, int publisherClientNumber, int subscriberClientNumber,
                        int messageNumber, String topic, int qos, int rateInMillis, int awaitTerminationInSecs)
                        throws MqttException, InterruptedException, IOException {

                List<AsyncClient> publisherClientList = ClientUtils.createAsyncClients(publisherClientNumber, broker,
                                clientIdBase, clientType, firstClientIdNumber, clientPassword);
                List<AsyncClient> subscriberClientList = ClientUtils.createAsyncClients(subscriberClientNumber, broker,
                                clientIdBase, clientType, publisherClientNumber + firstClientIdNumber, clientPassword);

                System.out.printf(CLIENT_CONN_STARTED_MSG, Instant.now().toString());
                ClientUtils.connect(subscriberClientList);
                ClientUtils.connect(publisherClientList);
                System.out.printf(CLIENT_CONN_ENDED_MSG, Instant.now().toString());

                System.out.printf(CLIENT_SUB_STARTED_MSG, Instant.now().toString());
                ClientUtils.subscribe(subscriberClientList, topic, qos);
                System.out.printf(CLIENT_SUB_ENDED_MSG, Instant.now().toString());

                Instant sendingTime = Instant.now();
                if (publisherClientList.size() > 0) {
                        int schedulerNumber = 9;
                        int initDelayInMillis = 0;
                        ScheduledFuture<?>[] schedulers = new ScheduledFuture<?>[schedulerNumber];
                        for (int i = 0; i < schedulerNumber; i++) {
                                schedulers[i] = executorServiceHandler.scheduleAtFixedRate(publisherClientList,
                                                messageNumber, qos, topic, initDelayInMillis, rateInMillis,
                                                awaitTerminationInSecs);
                                initDelayInMillis += ONE_MINUTE_IN_MILLIS;
                        }

                        executorServiceHandler.waitThenCancelSchedulers(schedulers, messageNumber);
                        executorServiceHandler.shutdownExecutorService(awaitTerminationInSecs);

                        Instant deliveryCompleteTime = Instant.now();
                        Duration timeElapsed = Duration.between(sendingTime, deliveryCompleteTime);

                        System.out.printf(CLIENT_DISCONN_STARTED_MSG, Instant.now().toString());
                        ClientUtils.disconnect(publisherClientList);
                        ClientUtils.disconnect(subscriberClientList);
                        System.out.printf(CLIENT_DISCONN_ENDED_MSG, Instant.now().toString());

                        System.out.printf(GETTING_RESULTS_MSG);
                        Utils.writeDelayValuesToFile(publisherClientList);
                        Utils.writeResultsToFile(subscriberClientList, publisherClientList, qos, messageNumber,
                                        executorServiceHandler.getNumberOfSentMessages(), timeElapsed);
                } else {
                        System.out.printf(NO_PUBLISHER_MSG);
                        waitForAllMessagesArrive(subscriberClientList, messageNumber);
                }
        }
}
