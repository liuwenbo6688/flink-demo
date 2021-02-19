package com.datax.portrait.carrier;

/**
 * Created by li on 2019/1/5.
 */
public class CarrierInfo {


    private String carrier;//运营商

    private Long count;//数量

    private String groupField;//分组


    public String getCarrier() {
        return carrier;
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getGroupField() {
        return groupField;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }
}
