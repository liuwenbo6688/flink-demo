package com.datax.portrait.yearbase;

import com.datax.util.DateUtils;
import com.datax.util.HbaseUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 *
 */
public class YearBaseMap implements MapFunction<String, YearBase> {


    @Override
    public YearBase map(String s) throws Exception {


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
        String useType = userinfos[7]; //'终端类型：0、pc端；1、移动端'


        /**
         * 1 计算单个用户的标签，写入hbase
         */
        String yearBaseType = DateUtils.getYearBaseByAge(age);

        String tablename = "user_flag_info";
        String rowkey = userId;
        String famliyname = "base_info";
        String column = "year_base";  // 年代列

        HbaseUtils.putdata(tablename, rowkey, famliyname, column, yearBaseType); // 年代标签
        HbaseUtils.putdata(tablename, rowkey, famliyname, "age", age); // 年龄值属性


        /**
         *
         */
        // reduce聚合统计每个标签的数量
        YearBase yearBase = new YearBase();
        String groupField = "yearbase==" + yearBaseType;
        yearBase.setYearType(yearBaseType);
        yearBase.setCount(1l);
        yearBase.setGroupField(groupField);


        return yearBase;
    }
}
