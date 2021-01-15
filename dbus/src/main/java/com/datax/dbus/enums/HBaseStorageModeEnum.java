package com.datax.dbus.enums;




public enum HBaseStorageModeEnum implements CodeEnum{
    /**
     * STRING
     */
    STRING(0, "STRING"),
    /**
     * NATIVE
     */
    NATIVE(1, "NATIVE"),
    /**
     * PHOENIX
     */
    PHOENIX(2, "PHOENIX");

    private Integer code;

    private String message;

    HBaseStorageModeEnum(Integer code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public Integer getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
