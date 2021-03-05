package com.szalaie.loadtest;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class App {

    public static void main(String[] args) {
        String broker = "tcp://0.0.0.0:1883";
        String clientId = "test01";
        String subscriberClientId = "test02";
        String password = "passw";
        String topic = "/device/test_type/test01";
        int messageNumber = 200;
        int qos = 1;
        int awaitTerminationInSecs = 60;

        try {
            final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(messageNumber);
            final AtomicInteger count = new AtomicInteger(0);
            MemoryPersistence persistence = new MemoryPersistence();
            Client subscriber = new Client(broker, subscriberClientId, password, persistence);
            Client publisher = new Client(broker, clientId, password, persistence);

            publisher.connect();
            subscriber.connect();
            subscriber.subscribe(topic, qos);

            final ScheduledFuture<?> publishHandler = executorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        count.getAndIncrement();
                        publisher.publishWithTimePayload(topic, qos, false);
                    } catch (MqttException e) {
                        e.printStackTrace();
                    }
                }
            }, 0, 1, TimeUnit.MICROSECONDS);

            while (true) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (count.get() >= messageNumber) {
                    System.out.println("Cancelling publishHandler");
                    publishHandler.cancel(false);

                    System.out.printf("Waiting %d seconds to terminate\n", awaitTerminationInSecs);
                    executorService.awaitTermination(awaitTerminationInSecs, TimeUnit.SECONDS);

                    System.out.println("Shutting down ScheduledExecutorService");
                    executorService.shutdown();
                    break;
                }
            }

            if (!executorService.awaitTermination(awaitTerminationInSecs, TimeUnit.SECONDS)) {
                System.out.println("Shut down now ScheduledExecutorService");
                executorService.shutdownNow();
            }

            publisher.disconnect();
            subscriber.disconnect();

            System.out.println("Getting results");
            List<Long> latencies = subscriber.getLatencies();
            Utils.writeResultToFile(latencies, qos, messageNumber, count.get(), publisher);
        } catch (MqttException | InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
