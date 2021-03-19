package com.szalaie.loadtest;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Utils {

    // https://stackoverflow.com/questions/4485128/how-do-i-convert-long-to-byte-and-back-in-java
    public static byte[] longToBytes(long l) {
        byte[] result = new byte[Long.BYTES];
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            result[i] = (byte) (l & 0xFF);
            l >>= Byte.SIZE;
        }
        return result;
    }

    public static long bytesToLong(final byte[] b) {
        long result = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (b[i] & 0xFF);
        }
        return result;
    }

    public static long calculateLatency(long receivingTime, byte[] sendingTime) {
        return receivingTime - bytesToLong(sendingTime);
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
            int messageNumber) throws IOException {
        List<Long> latencies = writeLatenciesToFile(subscriberClientList);

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyy-MM-dd_hh-mm-ss");
        String resultFilePath = "results" + File.separator + "results" + dateTimeFormatter.format(ZonedDateTime.now())
                + ".txt";
        File resultFile = new File(resultFilePath);
        FileWriter resultFileWriter = createFileWriter(resultFile);
        PrintWriter printWriter = new PrintWriter(resultFileWriter);

        OptionalDouble averageLatency = calculateAverageLatency(latencies);
        int successfullySentMessagesNumber = sumSuccessfullySentMessagesNumber(publisherClientList);

        printWriter.print("---- RESULT ----\n");
        printWriter.printf("Average latency %f \n", averageLatency.isEmpty() ? 0.0 : averageLatency.getAsDouble());
        printWriter.printf("QoS: %d, messages: %d \n", qos, messageNumber);
        printWriter.printf("Successfully sent messages: %d, received messages: %d \n", successfullySentMessagesNumber,
                latencies.size());
        printWriter.close();
    }

    public static List<Long> writeLatenciesToFile(List<Client> subscriberClientList) throws IOException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyy-MM-dd_hh-mm-ss");
        String latenciesFilePath = "results" + File.separator + "latencies"
                + dateTimeFormatter.format(ZonedDateTime.now()) + ".txt";
        File latenciesFile = new File(latenciesFilePath);
        FileWriter latenciesFileWriter = createFileWriter(latenciesFile);
        PrintWriter latenciesPrintWriter = new PrintWriter(latenciesFileWriter);
        int receivedMessagesNumber = 0;
        List<Long> newList = new LinkedList<>();

        latenciesPrintWriter.print("latency in ms:\n");
        for (Client subscriber : subscriberClientList) {
            List<Long> latencies = subscriber.getLatencies();
            newList = Stream.concat(latencies.stream(), newList.stream()).collect(Collectors.toList());

            receivedMessagesNumber += latencies.size();
            latencies.forEach((latency) -> {
                latenciesPrintWriter.print(latency + ";\n");
            });
        }

        latenciesPrintWriter.printf("receivedMessagesNumber: %d\n", receivedMessagesNumber);

        latenciesPrintWriter.close();
        return newList;
    }
}
