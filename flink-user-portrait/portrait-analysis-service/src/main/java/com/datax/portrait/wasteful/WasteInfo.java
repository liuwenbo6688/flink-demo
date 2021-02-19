package com.datax.portrait.wasteful;


import java.util.List;

/**
 * Created by li on 2019/1/5.
 */
public class WasteInfo {


    private String wasteType; // 败家指数区段：0-20 、20-50 、50-70、70-80、80-90、90-100

    private String userId;

    private String createTime;

    private String payType;

    private String payTime;

    private String payStatus; // 0、未支付 1、已支付 2、已退款

    private String amount; // 支付金额
    private String couponAmount; // 优惠券金额
    private String totalAmount;  // 订单总金额
    private String refundAmount; // 退款金额

    private Long count; //数量

    private String groupField; //分组字段


    private List<WasteInfo> list;

    public List<WasteInfo> getList() {
        return list;
    }

    public void setList(List<WasteInfo> list) {
        this.list = list;
    }


    public String getWasteType() {
        return wasteType;
    }

    public void setWasteType(String wasteType) {
        this.wasteType = wasteType;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getPayType() {
        return payType;
    }

    public void setPayType(String payType) {
        this.payType = payType;
    }

    public String getPayTime() {
        return payTime;
    }

    public void setPayTime(String payTime) {
        this.payTime = payTime;
    }

    public String getPayStatus() {
        return payStatus;
    }

    public void setPayStatus(String payStatus) {
        this.payStatus = payStatus;
    }

    public String getCouponAmount() {
        return couponAmount;
    }

    public void setCouponAmount(String couponAmount) {
        this.couponAmount = couponAmount;
    }

    public String getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(String totalAmount) {
        this.totalAmount = totalAmount;
    }

    public String getRefundAmount() {
        return refundAmount;
    }

    public void setRefundAmount(String refundAmount) {
        this.refundAmount = refundAmount;
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
