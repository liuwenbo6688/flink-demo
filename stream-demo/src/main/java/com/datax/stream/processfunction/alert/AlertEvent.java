package com.datax.stream.processfunction.alert;


/**
 * Desc: alert event
 * Created by zhisheng on 2019/10/13 上午10:14
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */

public class AlertEvent {

    private String type;

    private MetricEvent metricEvent;

    private boolean recover;

    private Long trigerTime;

    private Long recoverTime;


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public MetricEvent getMetricEvent() {
        return metricEvent;
    }

    public void setMetricEvent(MetricEvent metricEvent) {
        this.metricEvent = metricEvent;
    }

    public boolean isRecover() {
        return recover;
    }

    public void setRecover(boolean recover) {
        this.recover = recover;
    }

    public Long getTrigerTime() {
        return trigerTime;
    }

    public void setTrigerTime(Long trigerTime) {
        this.trigerTime = trigerTime;
    }

    public Long getRecoverTime() {
        return recoverTime;
    }

    public void setRecoverTime(Long recoverTime) {
        this.recoverTime = recoverTime;
    }
}