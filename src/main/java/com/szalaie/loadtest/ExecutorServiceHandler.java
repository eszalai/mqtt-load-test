package com.szalaie.loadtest;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceHandler {

    final static String SHUT_DOWN_EXECUTOR_SERVICE_MSG = "Shutting down ExecutorService";
    final static String SHUT_DOWN_NOW_EXECUTOR_SERVICE_MSG = "Shut down now ExecutorService";
    final static String WAITING_TO_TERMINATE_EXECUTOR_SERVICE_MSG = "Waiting %d seconds to terminate\n";
    ExecutorService executorService;

    public ExecutorServiceHandler(ExecutorService executorService) {
        this.executorService = executorService;
    }

    void scheduleCommands(List<Runnable> commandList, int delayBetweenMessagesInMillisec) {
        int delay = 0;
        for (Runnable command : commandList) {
            ((ScheduledExecutorService) executorService).schedule(command, delay, TimeUnit.MILLISECONDS);
            delay += delayBetweenMessagesInMillisec;
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