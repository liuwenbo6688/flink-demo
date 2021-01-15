package com.datax.phd.model;



import java.io.Serializable;


public class Config implements Serializable {
    private static final long serialVersionUID = 4416826133199103653L;

    // 定义每个渠道的 购买次数 和 路径长度
    // {"channel":"APP", "registerDate":"2018-01-01", "historyPurchaseTimes":0, "maxPurchasePathLength":3}

    private String channel;
    private String registerDate;
    private Integer historyPurchaseTimes;
    private Integer maxPurchasePathLength;

    public Config() {
    }

    public Config(String channel, String registerDate, Integer historyPurchaseTimes, Integer maxPurchasePathLength) {
        this.channel = channel;
        this.registerDate = registerDate;
        this.historyPurchaseTimes = historyPurchaseTimes;
        this.maxPurchasePathLength = maxPurchasePathLength;
    }


    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getRegisterDate() {
        return registerDate;
    }

    public void setRegisterDate(String registerDate) {
        this.registerDate = registerDate;
    }

    public Integer getHistoryPurchaseTimes() {
        return historyPurchaseTimes;
    }

    public void setHistoryPurchaseTimes(Integer historyPurchaseTimes) {
        this.historyPurchaseTimes = historyPurchaseTimes;
    }

    public Integer getMaxPurchasePathLength() {
        return maxPurchasePathLength;
    }

    public void setMaxPurchasePathLength(Integer maxPurchasePathLength) {
        this.maxPurchasePathLength = maxPurchasePathLength;
    }

    @Override
    public String toString() {
        return "Config{" +
                "channel='" + channel + '\'' +
                ", registerDate='" + registerDate + '\'' +
                ", historyPurchaseTimes=" + historyPurchaseTimes +
                ", maxPurchasePathLength=" + maxPurchasePathLength +
                '}';
    }
}
