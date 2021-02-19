package com.datax.portrait.email;

import com.datax.util.EmailUtils;
import com.datax.util.HbaseUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by li on 2019/1/5.
 */
public class EmailMap implements MapFunction<String, EmaiInfo> {
    @Override
    public EmaiInfo map(String s) throws Exception {
        if (StringUtils.isBlank(s)) {
            return null;
        }


        String[] userinfos = s.split(",");

        String userId = userinfos[0];
        String userName = userinfos[1];
        String sex = userinfos[2];
        String telephone = userinfos[3];
        String email = userinfos[4];
        String age = userinfos[5];
        String registerTime = userinfos[6];
        String useType = userinfos[7];  //'终端类型：0、pc端；1、移动端；2、小程序端'

        /**
         * 解析 email 类型
         */
        String emailType = EmailUtils.getEmailType(email);


        /**
         * 标签存 hbase
         */
        String tablename = "user_flag_info";
        String rowkey = userId;
        String famliyname = "base_info";
        String column = "email_info"; //运营商
        HbaseUtils.putdata(tablename, rowkey, famliyname, column, emailType);


        /**
         *
         */
        EmaiInfo emailInfo = new EmaiInfo();
        String groupField = "emailInfo==" + emailType;
        emailInfo.setEmailType(emailType);
        emailInfo.setCount(1l);
        emailInfo.setGroupField(groupField);


        return emailInfo;
    }
}
