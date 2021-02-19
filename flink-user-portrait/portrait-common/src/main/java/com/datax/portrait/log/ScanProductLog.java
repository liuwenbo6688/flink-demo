package com.datax.portrait.log;

import java.io.Serializable;

/**
 * 浏览商品行为日志
 */
public class ScanProductLog implements Serializable {


    private int productId;   //商品id
    private int productTypeId;   //商品类别id
    private String scanTime;  //浏览时间
    private String stayTime;  //停留时间
    private int userId;   //用户id
    private int terminalType;   //终端类型：0、pc端；1、移动端；2、小程序端'
    private String ip;  // 用户ip

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

    public String getScanTime() {
        return scanTime;
    }

    public void setScanTime(String scanTime) {
        this.scanTime = scanTime;
    }

    public String getStayTime() {
        return stayTime;
    }

    public void setStayTime(String stayTime) {
        this.stayTime = stayTime;
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
