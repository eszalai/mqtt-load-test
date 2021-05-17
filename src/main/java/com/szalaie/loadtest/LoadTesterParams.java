package com.szalaie.loadtest;

public class LoadTesterParams {
    private String brokerUrl;
    private String clientIdBase;
    private String clientPassword;
    private String clientType;
    private int firstClientIdNumber;
    private int publisherNumber;
    private String topicToPublish;
    private int subscriberNumber;
    private String topicToSubscribe;
    private int messageNumber;
    private int qos;
    private int delayBetweenMessagesInMillisec;
    private int connectionTerminationInSecs;
    private int loadIncreasingNumber;
    private int loadIncreasingRate;

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public String getClientIdBase() {
        return clientIdBase;
    }

    public void setClientIdBase(String clientIdBase) {
        this.clientIdBase = clientIdBase;
    }

    public String getClientPassword() {
        return clientPassword;
    }

    public void setClientPassword(String clientPassword) {
        this.clientPassword = clientPassword;
    }

    public String getClientType() {
        return clientType;
    }

    public void setClientType(String clientType) {
        this.clientType = clientType;
    }

    public int getFirstClientIdNumber() {
        return firstClientIdNumber;
    }

    public void setFirstClientIdNumber(int firstClientIdNumber) {
        this.firstClientIdNumber = firstClientIdNumber;
    }

    public int getPublisherNumber() {
        return publisherNumber;
    }

    public void setPublisherNumber(int publisherNumber) {
        this.publisherNumber = publisherNumber;
    }

    public String getTopicToPublish() {
        return topicToPublish;
    }

    public void setTopicToPublish(String topicToPublish) {
        this.topicToPublish = topicToPublish;
    }

    public int getSubscriberNumber() {
        return subscriberNumber;
    }

    public void setSubscriberNumber(int subscriberNumber) {
        this.subscriberNumber = subscriberNumber;
    }

    public String getTopicToSubscribe() {
        return topicToSubscribe;
    }

    public void setTopicToSubscribe(String topicToSubscribe) {
        this.topicToSubscribe = topicToSubscribe;
    }

    public int getMessageNumber() {
        return messageNumber;
    }

    public void setMessageNumber(int messageNumber) {
        this.messageNumber = messageNumber;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public int getDelayBetweenMessagesInMillisec() {
        return delayBetweenMessagesInMillisec;
    }

    public void setDelayBetweenMessagesInMillisec(int delayBetweenMessagesInMillisec) {
        this.delayBetweenMessagesInMillisec = delayBetweenMessagesInMillisec;
    }

    public int getConnectionTerminationInSecs() {
        return connectionTerminationInSecs;
    }

    public void setConnectionTerminationInSecs(int connectionTerminationInSecs) {
        this.connectionTerminationInSecs = connectionTerminationInSecs;
    }

    public int getLoadIncreasingNumber() {
        return loadIncreasingNumber;
    }

    public void setLoadIncreasingNumber(int loadIncreasingNumber) {
        this.loadIncreasingNumber = loadIncreasingNumber;
    }

    public int getLoadIncreasingRate() {
        return loadIncreasingRate;
    }

    public void setLoadIncreasingRate(int loadIncreasingRate) {
        this.loadIncreasingRate = loadIncreasingRate;
    }
}
