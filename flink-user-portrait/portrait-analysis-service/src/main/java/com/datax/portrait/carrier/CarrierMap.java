package com.datax.portrait.carrier;

import com.datax.util.CarrierUtils;
import com.datax.util.HbaseUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 *
 */
public class CarrierMap implements MapFunction<String, CarrierInfo> {
    @Override
    public CarrierInfo map(String s) throws Exception {
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


        int carrierType = CarrierUtils.getCarrierByTelephone(telephone);
        String carrierTypeStr = carrierType == 0 ? "未知运营商" : carrierType == 1 ? "移动用户" : carrierType == 2 ? "联通用户" : "电信用户";


        /**
         * 标签保存 hbase
         */
        String tablename = "user_flag_info";
        String rowkey = userId;
        String famliyname = "base_info";
        String column = "carrier_info"; //运营商
        HbaseUtils.putdata(tablename, rowkey, famliyname, column, carrierTypeStr);


        /**
         * 返回 CarrierInfo
         */
        CarrierInfo carrierInfo = new CarrierInfo();
        String groupField = "carrierInfo==" + carrierType;
        carrierInfo.setCount(1l);
        carrierInfo.setCarrier(carrierTypeStr);
        carrierInfo.setGroupField(groupField);


        return carrierInfo;
    }
}
