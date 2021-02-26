package com.szalaie.loadtest;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
 
public class App {

    public static void main(String[] args) {
        String broker = "tcp:0.0.0.0:3000";
        String clientId = "test01";
        String subscriberClientId = "test02";
        String password = "passw";
        String topic = "/device/test_type/test01";
        MemoryPersistence persistence = new MemoryPersistence();
        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        try {
            Client subscriber = new Client(broker, subscriberClientId, password, persistence);
            Client publisher = new Client(broker, clientId, password, persistence);
            subscriber.connect();
            publisher.connect();
            subscriber.subscribe(topic, 1);
            int messageNumber = 20000;
            final AtomicInteger count = new AtomicInteger(0);

            executorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        count.getAndIncrement();
                        publisher.publishWithTimePayload(topic, 1, false);
                        if (count.get() >= messageNumber) {
                            executorService.shutdown();
                            if (!executorService.awaitTermination(180, TimeUnit.SECONDS)) {
                                executorService.shutdownNow();
                                try {
                                    publisher.disconnect();
                                } catch (Exception e) {
                                    System.out.println("error during disconnecting: " + e.getMessage());
                                }
                                subscriber.getResult();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, 0, 1, TimeUnit.MICROSECONDS);
            subscriber.getResult();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}
