package com.datax.dbus.model;

/**
 *
 * 订单
 */



import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;


public class Orders implements Serializable {
    private Integer orderId;

    private String orderNo;

    private Integer userId;

    private Integer goodId;

    private BigDecimal goodsMoney;

    private BigDecimal realTotalMoney;

    private Integer payFrom;

    private String province;

    private Timestamp createTime;


    public Integer getOrderId() {
        return orderId;
    }

    public void setOrderId(Integer orderId) {
        this.orderId = orderId;
    }

    public String getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getGoodId() {
        return goodId;
    }

    public void setGoodId(Integer goodId) {
        this.goodId = goodId;
    }

    public BigDecimal getGoodsMoney() {
        return goodsMoney;
    }

    public void setGoodsMoney(BigDecimal goodsMoney) {
        this.goodsMoney = goodsMoney;
    }

    public BigDecimal getRealTotalMoney() {
        return realTotalMoney;
    }

    public void setRealTotalMoney(BigDecimal realTotalMoney) {
        this.realTotalMoney = realTotalMoney;
    }

    public Integer getPayFrom() {
        return payFrom;
    }

    public void setPayFrom(Integer payFrom) {
        this.payFrom = payFrom;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }


    @Override
    public String toString() {
        return "Orders{" +
                "orderId=" + orderId +
                ", orderNo='" + orderNo + '\'' +
                ", userId=" + userId +
                ", goodId=" + goodId +
                ", goodsMoney=" + goodsMoney +
                ", realTotalMoney=" + realTotalMoney +
                ", payFrom=" + payFrom +
                ", province='" + province + '\'' +
                ", createTime=" + createTime +
                '}';
    }
}
