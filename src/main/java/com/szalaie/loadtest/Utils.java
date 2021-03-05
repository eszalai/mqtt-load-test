package com.szalaie.loadtest;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.OptionalDouble;

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

    public static void writeResultToFile(List<Long> latencies, int qos, int messagesNum, int sentMessagesNum,
            Client client) throws IOException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyy-dd-MM_hh-mm-ss");
        String resultFilePath = "results" + File.separator + dateTimeFormatter.format(ZonedDateTime.now()) + ".txt";
        File file = new File(resultFilePath);
        FileWriter fileWriter;

        if (file.exists()) {
            fileWriter = new FileWriter(resultFilePath, true);
        } else {
            file.getParentFile().mkdirs();
            fileWriter = new FileWriter(resultFilePath);
        }
        PrintWriter printWriter = new PrintWriter(fileWriter);
        OptionalDouble averageLatency = calculateAverageLatency(latencies);

        printWriter.print("---- RESULT ----\n");
        printWriter.printf("Average latency %f \n", averageLatency.isEmpty() ? 0.0 : averageLatency.getAsDouble());
        printWriter.printf("QoS: %d, messages: %d, sent messages: %d \n", qos, messagesNum, sentMessagesNum);
        printWriter.printf("Successfully sent messages: %d, received messages: %d \n",
                client.getSuccessfullySentMessagesNumber(), latencies.size());

        printWriter.print("latency in ms:\n");
        latencies.forEach((latency) -> {
            printWriter.print(latency + ";\n");
        });

        printWriter.close();
    }
}
