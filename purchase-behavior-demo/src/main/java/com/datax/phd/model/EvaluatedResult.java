package com.datax.phd.model;




import java.util.Map;

public class EvaluatedResult {
    //{"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","purchasePathLength":9,"eventTypeCounts":{"ADD_TO_CART":1,"PURCHASE":1,"VIEW_PRODUCT":7}}
    private String userId;
    private String channel;
    private Integer purchasePathLength;
    private Map<String, Integer> eventTypeCounts;


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Integer getPurchasePathLength() {
        return purchasePathLength;
    }

    public void setPurchasePathLength(Integer purchasePathLength) {
        this.purchasePathLength = purchasePathLength;
    }

    public Map<String, Integer> getEventTypeCounts() {
        return eventTypeCounts;
    }

    public void setEventTypeCounts(Map<String, Integer> eventTypeCounts) {
        this.eventTypeCounts = eventTypeCounts;
    }
}
