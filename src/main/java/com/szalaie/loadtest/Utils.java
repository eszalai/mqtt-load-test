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
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

public class Utils {

    final static String LATENCIES_FILE_PATH = "results%Slatencies%S%S-%S.csv";
    final static String RESULTS_FILE_PATH = "results%Sresults%S%S.txt";
    final static String AVERAGE_LATENCY_STR = "Average latency in nanoseconds: %f \n";
    final static String QOS_STR = "QoS: %d\n";
    final static String PUBLISHER_NUMBER_STR = "Number of publishers: %d\n";
    final static String SUBSCRIBER_NUMBER_STR = "Number of subscribers: %d\n";
    final static String MESSAGE_NUMBER_STR = "Number of messages: %d \n";
    final static String SENT_MESSAGE_NUMBER_STR = "Number of sent messages: %d \n";
    final static String SUCCESSFULLY_SENT_MESSAGE_NUMBER_STR = "Number of successfully sent messages: %d\n";
    final static String RECEIVED_MESSAGE_NUMBER_STR = "Number of received messages: %d \n";
    final static String RECEIVED_MESSAGE_PER_SUBSCRIBER_NUMBER_STR = "Number of received messages per subscriber: %d.%d \n";
    final static String LATENCIES_HEADER_STR = "sendingTime,arrivalTime,latencyInNanosec\n";
    final static String LATENCIES_LINE_FORMAT = "%S,%S,%d\n";
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

    public static List<Long> aggregateLatencies(List<Client> subsciberClientList) {
        List<Long> latencies = new ArrayList<>();
        subsciberClientList.forEach((subscriber) -> {
            latencies.addAll(subscriber.getLatencies());
        });
        return latencies;
    }

    public static OptionalDouble calculateAverageLatency(List<Long> latencies) {
        return latencies.stream().mapToDouble(a -> a).average();
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

    public static int sumSuccessfullySentMessagesNumber(List<Client> publisherClientList) {
        int successfullySentMessagesNumber = 0;
        for (Client client : publisherClientList) {
            successfullySentMessagesNumber += client.getSuccessfullySentMessagesNumber();
        }
        return successfullySentMessagesNumber;
    }

    public static void writeResultToFile(List<Client> subscriberClientList, List<Client> publisherClientList, int qos,
            int messageNumber, int numberOfSentMessages) throws IOException {
        int numberOfSubscribers = subscriberClientList.size();
        int numberOfPublishers = publisherClientList.size();

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT);
        String resultFilePath = String.format(RESULTS_FILE_PATH, File.separator, File.separator,
                dateTimeFormatter.format(ZonedDateTime.now()));
        File resultFile = new File(resultFilePath);
        FileWriter resultFileWriter = createFileWriter(resultFile);
        PrintWriter printWriter = new PrintWriter(resultFileWriter);

        List<Long> latencies = aggregateLatencies(subscriberClientList);
        OptionalDouble averageLatency = calculateAverageLatency(latencies);
        int successfullySentMessagesNumber = sumSuccessfullySentMessagesNumber(publisherClientList);
        int receivedMessagesPerSubscriberQuotient = numberOfSubscribers != 0 ? latencies.size() / numberOfSubscribers
                : 0;
        int receivedMessagesPerSubscriberRemainder = numberOfSubscribers != 0 ? latencies.size() % numberOfSubscribers
                : 0;

        printWriter.printf(AVERAGE_LATENCY_STR, averageLatency.isEmpty() ? 0.0 : averageLatency.getAsDouble());
        printWriter.printf(QOS_STR, qos);
        printWriter.printf(PUBLISHER_NUMBER_STR, numberOfPublishers);
        printWriter.printf(SUBSCRIBER_NUMBER_STR, numberOfSubscribers);
        printWriter.printf(MESSAGE_NUMBER_STR, messageNumber);
        printWriter.printf(SENT_MESSAGE_NUMBER_STR, numberOfSentMessages);
        printWriter.printf(SUCCESSFULLY_SENT_MESSAGE_NUMBER_STR, successfullySentMessagesNumber);
        printWriter.printf(RECEIVED_MESSAGE_NUMBER_STR, latencies.size());
        printWriter.printf(RECEIVED_MESSAGE_PER_SUBSCRIBER_NUMBER_STR, receivedMessagesPerSubscriberQuotient,
                receivedMessagesPerSubscriberRemainder);
        printWriter.close();
    }

    public static void writeLatenciesToFile(List<Client> subscriberClientList) throws IOException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT);

        for (Client subscriber : subscriberClientList) {
            String latenciesFilePath = String.format(LATENCIES_FILE_PATH, File.separator, File.separator,
                    subscriber.getClientId(), dateTimeFormatter.format(ZonedDateTime.now()));
            File latenciesFile = new File(latenciesFilePath);
            FileWriter latenciesFileWriter = createFileWriter(latenciesFile);
            PrintWriter latenciesPrintWriter = new PrintWriter(latenciesFileWriter);
            latenciesPrintWriter.print(LATENCIES_HEADER_STR);

            Map<Instant, byte[]> messageArrivalTimeAndPayloadMap = subscriber.getMessageArrivalTimeAndPayloadMap();

            for (Instant messageArrivalTime : messageArrivalTimeAndPayloadMap.keySet()) {
                String messageSendingTime = new String(messageArrivalTimeAndPayloadMap.get(messageArrivalTime),
                        StandardCharsets.UTF_8);
                long latency = Utils.calculateLatency(messageArrivalTime, messageSendingTime);
                latenciesPrintWriter.printf(LATENCIES_LINE_FORMAT, messageSendingTime, messageArrivalTime.toString(),
                        latency);
            }

            latenciesPrintWriter.close();
        }
    }
}
