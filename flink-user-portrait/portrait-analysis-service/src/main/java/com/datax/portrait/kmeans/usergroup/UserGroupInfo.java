package com.datax.portrait.kmeans.usergroup;


import java.util.List;

/**
 *
 */
public class UserGroupInfo {

    private String userId;

    private String createTime;

    private String amount;

    private String payType;

    private String payTime;

    private String payStatus;  //0、未支付 1、已支付 2、已退款

    private String couponAmount;

    private String totalAmount;

    private String refundAmount;

    private Long count;//数量

    private String productTypeId;//消费类目

    private String groupField; // 分组

    private List<UserGroupInfo> list; //一个用户所有的消费信息

    private double avgAmount; //平均消费金额
    private double maxAmount; //消费最大金额
    private int days; //消费频次, 平均多少天消费一次

    private Long buyType1; // 消费类目1数量
    private Long buyType2; // 消费类目2数量
    private Long buyType3; // 消费类目3数量

    private Long buyTime1; // 消费时间点1数量
    private Long buyTime2; // 消费时间点2数量
    private Long buyTime3; // 消费时间点3数量
    private Long buyTime4; // 消费时间点4数量

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

    public String getProductTypeId() {
        return productTypeId;
    }

    public void setProductTypeId(String productTypeId) {
        this.productTypeId = productTypeId;
    }

    public String getGroupField() {
        return groupField;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }

    public List<UserGroupInfo> getList() {
        return list;
    }

    public void setList(List<UserGroupInfo> list) {
        this.list = list;
    }

    public double getAvgAmount() {
        return avgAmount;
    }

    public void setAvgAmount(double avgAmount) {
        this.avgAmount = avgAmount;
    }

    public double getMaxAmount() {
        return maxAmount;
    }

    public void setMaxAmount(double maxAmount) {
        this.maxAmount = maxAmount;
    }

    public int getDays() {
        return days;
    }

    public void setDays(int days) {
        this.days = days;
    }

    public Long getBuyType1() {
        return buyType1;
    }

    public void setBuyType1(Long buyType1) {
        this.buyType1 = buyType1;
    }

    public Long getBuyType2() {
        return buyType2;
    }

    public void setBuyType2(Long buyType2) {
        this.buyType2 = buyType2;
    }

    public Long getBuyType3() {
        return buyType3;
    }

    public void setBuyType3(Long buyType3) {
        this.buyType3 = buyType3;
    }

    public Long getBuyTime1() {
        return buyTime1;
    }

    public void setBuyTime1(Long buyTime1) {
        this.buyTime1 = buyTime1;
    }

    public Long getBuyTime2() {
        return buyTime2;
    }

    public void setBuyTime2(Long buyTime2) {
        this.buyTime2 = buyTime2;
    }

    public Long getBuyTime3() {
        return buyTime3;
    }

    public void setBuyTime3(Long buyTime3) {
        this.buyTime3 = buyTime3;
    }

    public Long getBuyTime4() {
        return buyTime4;
    }

    public void setBuyTime4(Long buyTime4) {
        this.buyTime4 = buyTime4;
    }
}
