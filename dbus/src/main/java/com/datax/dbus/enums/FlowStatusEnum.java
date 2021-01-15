package com.datax.dbus.enums;



/**
 *
 */

public enum FlowStatusEnum implements CodeEnum {
    /**
     * 初始状态(新添加)
     */
    FLOWSTATUS_INIT(0, "初始状态"),
    /**
     * 就绪状态
     */
    FLOWSTATUS_READY(1, "就绪状态"),
    /**
     * 运行状态
     */
    FLOWSTATUS_RUNNING(2, "运行状态");

    private Integer code;

    private String message;

    FlowStatusEnum(Integer code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }}
