package com.datax.util;

/**
 * Created by li on 2019/1/5.
 */
public class EmailUtils {

    /**
     * 网易邮箱 @163.com @126.com
     * 移动邮箱 @139.com
     * 搜狐邮箱 @sohu.com
     * qq邮箱  @qq.com
     * 189邮箱 @189.cn
     * tom邮箱 @tom.com
     * 阿里邮箱 @aliyun.com
     * 新浪邮箱 @sina.com
     * 等等
     *
     * @param email
     * @return
     */
    public static String getEmailType(String email) {
        String emailType = "其他邮箱用户";
        if (email.contains("@163.com") || email.contains("@126.com")) {
            emailType = "网易邮箱用户";
        } else if (email.contains("@139.com")) {
            emailType = "移动邮箱用户";
        } else if (email.contains("@sohu.com")) {
            emailType = "搜狐邮箱用户";
        } else if (email.contains("@qq.com")) {
            emailType = "qq邮箱用户";
        } else if (email.contains("@189.cn")) {
            emailType = "189邮箱用户";
        } else if (email.contains("@tom.com")) {
            emailType = "tom邮箱用户";
        } else if (email.contains("@aliyun.com")) {
            emailType = "阿里邮箱用户";
        } else if (email.contains("@sina.com")) {
            emailType = "新浪邮箱用户";
        }
        return emailType;
    }
}
