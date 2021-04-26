package com.szalaie.loadtest;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutorServiceHandler {

    final static String SHUT_DOWN_EXECUTOR_SERVICE_MSG = "Shutting down ExecutorService %s%n";
    final static String SHUT_DOWN_NOW_EXECUTOR_SERVICE_MSG = "Shut down now ExecutorService %s%n";
    final static String WAITING_TO_TERMINATE_EXECUTOR_SERVICE_MSG = "Waiting %d seconds to terminate\n";
    final static String CANCELLING_SCHEDULER_MSG = "Cancelling scheduler";
    private ExecutorService executorService;
    private AtomicInteger messageCounter;
    private AtomicInteger clientIterator;

    public ExecutorServiceHandler(ExecutorService executorService) {
        this.executorService = executorService;
        this.messageCounter = new AtomicInteger(0);
    }

    public int getNumberOfSentMessages() {
        return this.messageCounter.get();
    }

    void scheduleCommands(List<Runnable> commandList, int delayBetweenMessagesInMillisec) {
        int delay = 0;
        for (Runnable command : commandList) {
            ((ScheduledThreadPoolExecutor) executorService).schedule(command, delay, TimeUnit.MILLISECONDS);
            delay += delayBetweenMessagesInMillisec;
            this.messageCounter.getAndIncrement();
        }
    }

    <T> ScheduledFuture<?> scheduleAtFixedRate(List<T> publisherClientList, int messageNumber, int qos,
            String topicToPublish, int initDelayInMillis, int delayBetweenMessagesInMillis,
            int awaitTerminationInSecs) {
        long period = delayBetweenMessagesInMillis;
        clientIterator = new AtomicInteger(0);
        ScheduledFuture<?> publishHandler = ((ScheduledThreadPoolExecutor) this.executorService)
                .scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            if (messageCounter.get() < messageNumber) {
                                int clientNumber = 0;

                                if (publisherClientList.size() > 1) {
                                    if (clientIterator.get() < publisherClientList.size()) {
                                        clientNumber = clientIterator.getAndIncrement();
                                    } else {
                                        clientIterator = new AtomicInteger(0);
                                    }
                                }

                                T publisher = publisherClientList.get(clientNumber);

                                if (messageCounter.get() < messageNumber) {
                                    if (publisher instanceof Client) {
                                        String topic = topicToPublish.length() == 0
                                                ? ((Client) publisher).getDefaultTopic()
                                                : topicToPublish;
                                        ((Client) publisher).publishWithTimePayload(topic, qos, false);
                                    } else if (publisher instanceof AsyncClient) {
                                        String topic = topicToPublish.length() == 0
                                                ? ((AsyncClient) publisher).getDefaultTopic()
                                                : topicToPublish;
                                        ((AsyncClient) publisher).publishWithTimePayload(topic, qos, false);
                                    }
                                    messageCounter.incrementAndGet();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }, initDelayInMillis, period, TimeUnit.MILLISECONDS);

        return publishHandler;
    }

    void waitThenCancelSchedulers(ScheduledFuture<?>[] schedulers, int messageNumber) {
        while (true) {
            if (messageCounter.get() >= messageNumber) {
                System.out.println(CANCELLING_SCHEDULER_MSG);
                for (ScheduledFuture<?> scheduler : schedulers) {
                    scheduler.cancel(false);
                }
                break;
            }
        }
    }

    Instant shutdownExecutorService(int awaitTerminationInSecs) throws InterruptedException {
        System.out.printf(WAITING_TO_TERMINATE_EXECUTOR_SERVICE_MSG, awaitTerminationInSecs);
        executorService.awaitTermination(awaitTerminationInSecs, TimeUnit.SECONDS);

        Instant shutDownDateTime = Instant.now();
        System.out.printf(SHUT_DOWN_EXECUTOR_SERVICE_MSG, shutDownDateTime.toString());
        executorService.shutdown();

        if (!executorService.awaitTermination(awaitTerminationInSecs, TimeUnit.SECONDS)) {
            shutDownDateTime = Instant.now();
            System.out.printf(SHUT_DOWN_NOW_EXECUTOR_SERVICE_MSG, shutDownDateTime.toString());
            executorService.shutdownNow();
        }
        return shutDownDateTime;
    }
}