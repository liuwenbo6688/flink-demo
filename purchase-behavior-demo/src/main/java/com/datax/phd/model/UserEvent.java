package com.datax.phd.model;



import java.io.Serializable;

/**
 *
 */
public class UserEvent implements Serializable {

    private static final long serialVersionUID = -52934893611845880L;

    // 用户行为日志
    /**
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12 09:27:11","data":{"productId":196}}
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"ADD_TO_CART","eventTime":"2018-06-12 09:43:18","data":{"productId":126}}
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12 09:27:11","data":{"productId":126}}
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"PURCHASE","eventTime":"2018-06-12 09:30:28","data":{"productId":196,"price":600.00,"amount":600.00}}
     */
    private String userId;
    private String channel;
    private String eventType;
    private long eventTime;
    private Product data;


    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

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

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public Product getData() {
        return data;
    }

    public void setData(Product data) {
        this.data = data;
    }


    @Override
    public String toString() {
        return "UserEvent{" +
                "userId='" + userId + '\'' +
                ", channel='" + channel + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                ", data=" + data +
                '}';
    }
}
