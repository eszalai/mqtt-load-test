package com.szalaie.loadtest;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutorServiceHandler {

    private final static String SHUT_DOWN_EXECUTOR_SERVICE_MSG = "Shutting down ExecutorService %s%n";
    private final static String SHUT_DOWN_NOW_EXECUTOR_SERVICE_MSG = "Shut down now ExecutorService %s%n";
    private final static String WAITING_TO_TERMINATE_EXECUTOR_SERVICE_MSG = "Waiting %d seconds to terminate\n";
    private final static String WAITING_TASKS_TO_COMPLETE = "Waiting for tasks to complete%n";
    private final static String WAITING_TASKS_TO_COMPLETE_ENDED = "Waiting for tasks to complete ended%n";
    private ExecutorService executorService;
    private AtomicInteger messageCounter;
    private AtomicInteger clientIterator;
    private CountDownLatch latch;

    public ExecutorServiceHandler(ExecutorService executorService) {
        this.executorService = executorService;
        this.messageCounter = new AtomicInteger(0);
        this.latch = new CountDownLatch(0);
    }

    public void setMessageNumber(int messageNumber) {
        this.latch = new CountDownLatch(messageNumber);
    }

    public int getNumberOfSentMessages() {
        return this.messageCounter.get();
    }

    <T> ScheduledFuture<?> scheduleAtFixedRate(List<T> publisherClientList, int qos, String topicToPublish,
            int initDelayInMillis, int delayBetweenMessagesInMillis) {
        clientIterator = new AtomicInteger(0);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    int clientNumber = 0;
                    if (publisherClientList.size() > 1) {
                        if (clientIterator.get() < publisherClientList.size()) {
                            clientNumber = clientIterator.getAndIncrement();
                        } else {
                            clientIterator = new AtomicInteger(0);
                        }
                    }

                    T publisher = publisherClientList.get(clientNumber);

                    if (latch.getCount() > 0) {
                        if (publisher instanceof Client) {
                            String topic = topicToPublish.length() == 0 ? ((Client) publisher).getDefaultTopic()
                                    : topicToPublish;
                            ((Client) publisher).publishWithTimePayload(topic, qos, false);
                        } else if (publisher instanceof AsyncClient) {
                            String topic = topicToPublish.length() == 0 ? ((AsyncClient) publisher).getDefaultTopic()
                                    : topicToPublish;
                            ((AsyncClient) publisher).publishWithTimePayload(topic, qos, false);
                        }
                        latch.countDown();
                        messageCounter.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        ScheduledFuture<?> publishHandler = ((ScheduledThreadPoolExecutor) this.executorService)
                .scheduleAtFixedRate(runnable, initDelayInMillis, delayBetweenMessagesInMillis, TimeUnit.MILLISECONDS);

        return publishHandler;
    }

    void waitingForAllMessagesToBeSent() throws InterruptedException {
        System.out.printf(WAITING_TASKS_TO_COMPLETE);
        latch.await();
        System.out.printf(WAITING_TASKS_TO_COMPLETE_ENDED);
    }

    void shutdownExecutorService(int awaitTerminationInSecs) throws InterruptedException {
        System.out.printf(WAITING_TO_TERMINATE_EXECUTOR_SERVICE_MSG, awaitTerminationInSecs);
        executorService.awaitTermination(awaitTerminationInSecs, TimeUnit.SECONDS);

        System.out.printf(SHUT_DOWN_EXECUTOR_SERVICE_MSG, Instant.now());
        executorService.shutdown();
        System.out.printf(SHUT_DOWN_NOW_EXECUTOR_SERVICE_MSG, Instant.now());
        executorService.shutdownNow();
    }
}