package com.datax.portrait.terminal;

/**
 * 使用终端实体
 */
public class TerminalInfo {


    private String terminalType; // 终端类型
    private long count;
    private String groupField;


    public String getTerminalType() {
        return terminalType;
    }

    public void setTerminalType(String terminalType) {
        this.terminalType = terminalType;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getGroupField() {
        return groupField;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }
}
