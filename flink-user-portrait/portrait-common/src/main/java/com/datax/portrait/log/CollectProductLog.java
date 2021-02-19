package com.datax.portrait.log;

/**
 * 收藏行为日志
 */
public class CollectProductLog {


    private int productId;  //商品id
    private int productTypeId;  //商品类别id

    private String operateTime;  //操作时间
    private int operateType;  //操作类型，0、收藏，1、取消

    private int userId;  //用户id
    private int terminalType;  //终端类型：0、pc端；1、移动端；2、小程序端'
    private String ip;   // 用户ip

    private String brand;  //品牌

    public int getProductId() {
        return productId;
    }

    public void setProductId(int productId) {
        this.productId = productId;
    }

    public int getProductTypeId() {
        return productTypeId;
    }

    public void setProductTypeId(int productTypeId) {
        this.productTypeId = productTypeId;
    }

    public String getOperateTime() {
        return operateTime;
    }

    public void setOperateTime(String operateTime) {
        this.operateTime = operateTime;
    }

    public int getOperateType() {
        return operateType;
    }

    public void setOperateType(int operateType) {
        this.operateType = operateType;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getTerminalType() {
        return terminalType;
    }

    public void setTerminalType(int terminalType) {
        this.terminalType = terminalType;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }
}
