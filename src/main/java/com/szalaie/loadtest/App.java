package com.szalaie.loadtest;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class App {

    static final String CONF_PROP_FILE = "config.properties";
    static final String BROKER_URL_PROP = "broker.url";
    static final String CLIENT_ID_BASE_PROP = "client.id.base";
    static final String CLIENT_PASSWORD_PROP = "client.password";
    static final String CLIENT_TYPE_PROP = "client.type";
    static final String CLIENT_ID_FIRST_PROP = "client.id.first";
    static final String PUBLISHER_NUMBER_PROP = "publisher.number";
    static final String PUBLISHER_TOPIC_PROP = "publisher.topic";
    static final String SUBSCRIBER_NUMBER_PROP = "subscriber.number";
    static final String SUBSCRIBER_TOPIC_PROP = "subscriber.topic";
    static final String MESSAGE_NUMBER_PROP = "message.number";
    static final String MESSAGE_QOS_PROP = "message.qos";
    static final String MESSAGE_DELAY_MILLIS_PROP = "message.delay.millis";
    static final String CONNECTION_TERMINATION_SECS_PROP = "connection.termination.secs";
    static final int CORE_POOL_SIZE = 30;

    public static void main(String[] args) {
        String broker;
        String clientIdBase;
        String clientPassword;
        String clientType;
        int firstClientIdNumber;
        int publisherNumber;
        String topicToPublish;
        int subscriberNumber;
        String topicToSubscribe;
        int messageNumber;
        int qos;
        int delayBetweenMessagesInMillisec;
        int connectionTerminationInSecs;

        try {
            InputStream input = App.class.getClassLoader().getResourceAsStream(CONF_PROP_FILE);
            Properties prop = new Properties();

            prop.load(input);

            broker = prop.getProperty(BROKER_URL_PROP);
            clientIdBase = prop.getProperty(CLIENT_ID_BASE_PROP);
            clientPassword = prop.getProperty(CLIENT_PASSWORD_PROP);
            clientType = prop.getProperty(CLIENT_TYPE_PROP);
            firstClientIdNumber = Integer.parseInt(prop.getProperty(CLIENT_ID_FIRST_PROP));
            publisherNumber = Integer.parseInt(prop.getProperty(PUBLISHER_NUMBER_PROP));
            topicToPublish = prop.getProperty(PUBLISHER_TOPIC_PROP);
            subscriberNumber = Integer.parseInt(prop.getProperty(SUBSCRIBER_NUMBER_PROP));
            topicToSubscribe = prop.getProperty(SUBSCRIBER_TOPIC_PROP);
            messageNumber = Integer.parseInt(prop.getProperty(MESSAGE_NUMBER_PROP));
            qos = Integer.parseInt(prop.getProperty(MESSAGE_QOS_PROP));
            delayBetweenMessagesInMillisec = Integer.parseInt(prop.getProperty(MESSAGE_DELAY_MILLIS_PROP));
            connectionTerminationInSecs = Integer.parseInt(prop.getProperty(CONNECTION_TERMINATION_SECS_PROP));

            final ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(CORE_POOL_SIZE);
            LoadTester loadTester = new LoadTester(executorService);

            loadTester.publishMessagesAsynchWithRate(broker, clientIdBase, firstClientIdNumber, clientType,
                    clientPassword, publisherNumber, subscriberNumber, messageNumber, topicToSubscribe, topicToPublish,
                    qos, delayBetweenMessagesInMillisec, connectionTerminationInSecs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
