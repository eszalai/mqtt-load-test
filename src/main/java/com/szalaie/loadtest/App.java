package com.szalaie.loadtest;

import java.util.concurrent.ScheduledThreadPoolExecutor;

public class App {

    public static void main(String[] args) {
        String broker = "tcp://localhost:1883";
        String clientIdBase = "device";
        String clientType = "test_type";
        String topic = "/device/test_type";
        int publisherClientNumber = 1;
        int subscriberClientNumber = 800;
        String clientPassword = "password";
        int delayBetweenMessagesInMillisec = 500;
        int messageNumber = 1000;
        int qos = 1;
        int awaitTerminationInSecs = 20;
        int firstClientIdNumber = 1;

        try {
            final ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(messageNumber);
            LoadTester loadTester = new LoadTester(executorService);

            loadTester.publishMessagesAsynchWithRate(broker, clientIdBase, firstClientIdNumber, clientType,
                    clientPassword, publisherClientNumber, subscriberClientNumber, messageNumber, topic, qos,
                    delayBetweenMessagesInMillisec, awaitTerminationInSecs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
