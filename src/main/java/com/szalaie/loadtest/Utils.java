package com.szalaie.loadtest;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {

    final static String EMPTY_STR = "";
    final static String DELAY_VALUES_FILE_PATH = "results%Sdelays%S%S-%S.csv";
    final static String RESULTS_FILE_PATH = "results%Sresults%S%S.txt";
    final static String QOS_STR = "QoS: %d\n";
    final static String PUBLISHER_NUMBER_STR = "Number of publishers: %d\n";
    final static String SUBSCRIBER_NUMBER_STR = "Number of subscribers: %d\n";
    final static String MESSAGE_NUMBER_STR = "Number of messages: %d \n";
    final static String SENT_MESSAGE_NUMBER_STR = "Number of sent messages: %d \n";
    final static String SUCCESSFULLY_SENT_MESSAGE_NUMBER_STR = "Number of successfully sent messages: %d\n";
    final static String MESSAGE_SENDING_DURATION_STR = "Message sending duration: %d seconds%n";
    final static String RATE_OF_SUCCESSFULLY_PROC_MESSAGES_STR = "Rate of successfully processed PUBLISH messages: %.3f%n";
    final static String MESSAGE_PROC_PER_SEC_STR = "Number of PUBLISH messages processed per second: %.3f%n";
    final static String AVERAGE_PROC_TIME_STR = "Average PUBLISH processing time in millis: %.3f%n";
    final static String MIN_PROC_TIME_STR = "Min PUBLISH processing time in millis: %d%n";
    final static String MAX_PROC_TIME_STR = "Max PUBLISH processing time in millis: %d%n";
    final static String DELAY_VALUES_FILE_HEADER_STR = "messageId,sendingTime,deliveryTime,durationBetweenInMillis\n";
    final static String DELAY_VALUES_FILE_LINE_FORMAT = "%S,%S,%S,%d\n";
    final static String DATE_TIME_FORMAT = "yyy-MM-dd_HH-mm-ss";

    public static long calculateLatency(Instant receivingTime, String sendingTime) {
        Instant sendingTimeInstant = Instant.parse(sendingTime);
        Duration timeElapsed = Duration.between(sendingTimeInstant, receivingTime);
        return timeElapsed.toNanos();
    }

    public static long calculateLatency(Instant receivingTime, byte[] sendingTime) {
        String messageSendingTime = new String(sendingTime, StandardCharsets.UTF_8);
        return calculateLatency(receivingTime, messageSendingTime);
    }

    public static <T> List<Long> aggregateDelayValues(List<T> publisherClientList) {
        List<Long> delays = new ArrayList<>();
        publisherClientList.forEach((publisher) -> {
            if (publisher instanceof Client) {
                delays.addAll(((Client) publisher).getDelays());
            } else if (publisher instanceof AsyncClient) {
                delays.addAll(((AsyncClient) publisher).getDelays());
            }
        });
        return delays;
    }

    public static double calculateProcessedMessagesPerSecond(int successfullySentMessagesNumber,
            double messageSendingDuration) {
        return messageSendingDuration != 0 ? ((double) successfullySentMessagesNumber / messageSendingDuration) : 0.0;
    }

    public static double calculatePercentage(double obtainedValue, double total) {
        return 100 * obtainedValue / total;
    }

    public static Double calculateAverage(List<Long> list) {
        return list.stream().mapToDouble(a -> a).average().getAsDouble();
    }

    public static long getMin(List<Long> list) {
        return list.stream().mapToLong(a -> a).min().getAsLong();
    }

    public static long getMax(List<Long> list) {
        return list.stream().mapToLong(a -> a).max().getAsLong();
    }

    public static FileWriter createFileWriter(File file) throws IOException {
        FileWriter fileWriter;
        if (file.exists()) {
            fileWriter = new FileWriter(file, true);
        } else {
            file.getParentFile().mkdirs();
            fileWriter = new FileWriter(file);
        }

        return fileWriter;
    }

    public static <T> int sumSuccessfullySentMessagesNumber(List<T> publisherClientList) {
        int successfullySentMessagesNumber = 0;
        for (T client : publisherClientList) {
            if (client instanceof Client) {
                successfullySentMessagesNumber += ((Client) client).getSuccessfullySentMessagesNumber();
            } else if (client instanceof AsyncClient) {
                successfullySentMessagesNumber += ((AsyncClient) client).getSuccessfullySentMessagesNumber();
            }
        }
        return successfullySentMessagesNumber;
    }

    public static <T> void writeResultsToFile(List<T> subscriberClientList, List<T> publisherClientList, int qos,
            int messageNumber, int numberOfSentMessages, Duration messageSendingDuration) throws IOException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT);
        String resultFilePath = String.format(RESULTS_FILE_PATH, File.separator, File.separator,
                dateTimeFormatter.format(ZonedDateTime.now()));
        File resultFile = new File(resultFilePath);
        FileWriter resultFileWriter = createFileWriter(resultFile);
        PrintWriter printWriter = new PrintWriter(resultFileWriter);

        int numberOfSubscribers = subscriberClientList.size();
        int numberOfPublishers = publisherClientList.size();
        List<Long> delays = aggregateDelayValues(publisherClientList);
        double averageDelay = calculateAverage(delays);
        int successfullySentMessagesNumber = sumSuccessfullySentMessagesNumber(publisherClientList);
        double rateOfSuccessfullyProcessedMessages = calculatePercentage(successfullySentMessagesNumber,
                numberOfSentMessages);
        long minDelay = getMin(delays);
        long maxDelay = getMax(delays);
        double processedMessagesPerSecond = calculateProcessedMessagesPerSecond(successfullySentMessagesNumber,
                messageSendingDuration.toSeconds());

        printWriter.printf(QOS_STR, qos);
        printWriter.printf(PUBLISHER_NUMBER_STR, numberOfPublishers);
        printWriter.printf(SUBSCRIBER_NUMBER_STR, numberOfSubscribers);
        printWriter.printf(MESSAGE_NUMBER_STR, messageNumber);
        printWriter.printf(SENT_MESSAGE_NUMBER_STR, numberOfSentMessages);
        printWriter.printf(SUCCESSFULLY_SENT_MESSAGE_NUMBER_STR, successfullySentMessagesNumber);

        printWriter.printf(MESSAGE_SENDING_DURATION_STR, messageSendingDuration.toSeconds());
        printWriter.printf(RATE_OF_SUCCESSFULLY_PROC_MESSAGES_STR, rateOfSuccessfullyProcessedMessages);
        printWriter.printf(MESSAGE_PROC_PER_SEC_STR, processedMessagesPerSecond);

        printWriter.printf(AVERAGE_PROC_TIME_STR, averageDelay);
        printWriter.printf(MIN_PROC_TIME_STR, minDelay);
        printWriter.printf(MAX_PROC_TIME_STR, maxDelay);
        printWriter.close();
    }

    public static <T> void writeDelayValuesToFile(List<T> publisherClientList) throws IOException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT);

        for (T publisher : publisherClientList) {
            String clientId = EMPTY_STR;
            Map<Integer, Instant> deliveryCompleteTimeByMessageId = new HashMap<>();
            Map<Integer, Instant> sendingMessageTimeByMessageId = new HashMap<>();

            if (publisher instanceof Client) {
                clientId = ((Client) publisher).getClientId();
                deliveryCompleteTimeByMessageId = ((Client) publisher).getDeliveryCompleteTimeByMessageId();
                sendingMessageTimeByMessageId = ((Client) publisher).getSendingMessageTimeByMessageId();
            } else if (publisher instanceof AsyncClient) {
                clientId = ((AsyncClient) publisher).getClientId();
                deliveryCompleteTimeByMessageId = ((AsyncClient) publisher).getDeliveryCompleteTimeByMessageId();
                sendingMessageTimeByMessageId = ((AsyncClient) publisher).getSendingMessageTimeByMessageId();
            }

            String delayValuesFilePath = String.format(DELAY_VALUES_FILE_PATH, File.separator, File.separator, clientId,
                    dateTimeFormatter.format(ZonedDateTime.now()));
            File delayValuesFile = new File(delayValuesFilePath);
            FileWriter delayValuesFileWriter = createFileWriter(delayValuesFile);
            PrintWriter delayValuesPrintWriter = new PrintWriter(delayValuesFileWriter);
            delayValuesPrintWriter.print(DELAY_VALUES_FILE_HEADER_STR);

            for (int messageId : sendingMessageTimeByMessageId.keySet()) {
                Instant sendingTime = sendingMessageTimeByMessageId.get(messageId);
                Instant deliveryCompleteTime = deliveryCompleteTimeByMessageId.get(messageId);
                String deliveryCompleteTimeString = EMPTY_STR;
                long timeElapsedInMillis = -1;
                if (deliveryCompleteTime != null) {
                    Duration timeElapsed = Duration.between(sendingTime, deliveryCompleteTime);
                    timeElapsedInMillis = timeElapsed.toMillis();
                    deliveryCompleteTimeString = deliveryCompleteTime.toString();
                }

                delayValuesPrintWriter.printf(DELAY_VALUES_FILE_LINE_FORMAT, messageId, sendingTime.toString(),
                        deliveryCompleteTimeString, timeElapsedInMillis);
            }

            delayValuesPrintWriter.close();
        }
    }
}
