package com.szalaie.loadtest;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
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
    static final String LOAD_INCREASING_NUMBER_PROP = "load.increasing.number";
    static final String LOAD_INCREASING_RATE_PROP = "load.increasing.rate";
    static final int CORE_POOL_SIZE = 30;

    static LoadTester loadTester;

    private static LoadTesterParams readProperties() throws IOException {
        InputStream input = App.class.getClassLoader().getResourceAsStream(CONF_PROP_FILE);
        Properties prop = new Properties();
        LoadTesterParams loadTesterParams = new LoadTesterParams();

        prop.load(Objects.requireNonNull(input));

        loadTesterParams.setBrokerUrl(prop.getProperty(BROKER_URL_PROP));
        loadTesterParams.setClientIdBase(prop.getProperty(CLIENT_ID_BASE_PROP));
        loadTesterParams.setClientPassword(prop.getProperty(CLIENT_PASSWORD_PROP));
        loadTesterParams.setClientType(prop.getProperty(CLIENT_TYPE_PROP));
        loadTesterParams.setFirstClientIdNumber(Utils.getIntegerPropertyValue(prop, CLIENT_ID_FIRST_PROP));
        loadTesterParams.setPublisherNumber(Utils.getIntegerPropertyValue(prop, PUBLISHER_NUMBER_PROP));
        loadTesterParams.setTopicToPublish(prop.getProperty(PUBLISHER_TOPIC_PROP));
        loadTesterParams.setSubscriberNumber(Utils.getIntegerPropertyValue(prop, SUBSCRIBER_NUMBER_PROP));
        loadTesterParams.setTopicToSubscribe(prop.getProperty(SUBSCRIBER_TOPIC_PROP));
        loadTesterParams.setMessageNumber(Utils.getIntegerPropertyValue(prop, MESSAGE_NUMBER_PROP));
        loadTesterParams.setQos(Utils.getIntegerPropertyValue(prop, MESSAGE_QOS_PROP));
        loadTesterParams
                .setDelayBetweenMessagesInMillisec(Utils.getIntegerPropertyValue(prop, MESSAGE_DELAY_MILLIS_PROP));
        loadTesterParams
                .setConnectionTerminationInSecs(Utils.getIntegerPropertyValue(prop, CONNECTION_TERMINATION_SECS_PROP));
        loadTesterParams.setLoadIncreasingNumber(Utils.getIntegerPropertyValue(prop, LOAD_INCREASING_NUMBER_PROP));
        loadTesterParams.setLoadIncreasingRate(Utils.getIntegerPropertyValue(prop, LOAD_INCREASING_RATE_PROP));

        return loadTesterParams;
    }

    public static void main(String[] args) {
        try {
            LoadTesterParams loadTesterParams = readProperties();

            final ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(CORE_POOL_SIZE);
            loadTester = new LoadTester(executorService);

            loadTester.runTest(loadTesterParams);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
