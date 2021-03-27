package com.szalaie.loadtest;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutorServiceHandler {

    final static String SHUT_DOWN_EXECUTOR_SERVICE_MSG = "Shutting down ExecutorService";
    final static String SHUT_DOWN_NOW_EXECUTOR_SERVICE_MSG = "Shut down now ExecutorService";
    final static String WAITING_TO_TERMINATE_EXECUTOR_SERVICE_MSG = "Waiting %d seconds to terminate\n";
    final static String CANCELLING_PUBLISH_HANDLER_MSG = "Cancelling publishHandler";
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
            ((ScheduledExecutorService) executorService).schedule(command, delay, TimeUnit.MILLISECONDS);
            delay += delayBetweenMessagesInMillisec;
            this.messageCounter.getAndIncrement();
        }
    }

    void scheduleAtFixedRate(List<Client> publisherList, int messageNumber, int qos, String topic,
            int delayBetweenMessagesInMillisec, int awaitTerminationInSecs) {
        long period = delayBetweenMessagesInMillisec;
        long initDelay = 0;
        clientIterator = new AtomicInteger(0);
        ScheduledFuture<?> publishHandler = ((ScheduledExecutorService) this.executorService)
                .scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            if (messageCounter.get() < messageNumber) {
                                int clientNumber = 0;
                                if (clientIterator.get() < publisherList.size()) {
                                    clientNumber = clientIterator.getAndIncrement();
                                } else {
                                    clientIterator = new AtomicInteger(0);
                                }
                                Client publisher = publisherList.get(clientNumber);

                                messageCounter.incrementAndGet();
                                publisher.publishWithTimePayload(topic, qos, false);
                            } else {
                                shutdownExecutorService(awaitTerminationInSecs);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }, initDelay, period, TimeUnit.MILLISECONDS);

        while (true) {
            if (messageCounter.get() >= messageNumber) {
                System.out.println(CANCELLING_PUBLISH_HANDLER_MSG);
                publishHandler.cancel(false);
                break;
            }
        }
    }

    void shutdownExecutorService(int awaitTerminationInSecs) throws InterruptedException {
        System.out.printf(WAITING_TO_TERMINATE_EXECUTOR_SERVICE_MSG, awaitTerminationInSecs);
        executorService.awaitTermination(awaitTerminationInSecs, TimeUnit.SECONDS);

        System.out.println(SHUT_DOWN_EXECUTOR_SERVICE_MSG);
        executorService.shutdown();

        if (!executorService.awaitTermination(awaitTerminationInSecs, TimeUnit.SECONDS)) {
            System.out.println(SHUT_DOWN_NOW_EXECUTOR_SERVICE_MSG);
            executorService.shutdownNow();
        }
    }
}