package com.datax.portrait.email;

/**
 * Created by li on 2019/1/5.
 */
public class EmaiInfo {


    private String emailType; //邮箱类型

    private Long count; //数量

    private String groupField; //分组字段


    public String getEmailType() {
        return emailType;
    }

    public void setEmailType(String emailType) {
        this.emailType = emailType;
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
