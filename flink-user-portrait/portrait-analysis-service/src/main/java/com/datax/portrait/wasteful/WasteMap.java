package com.datax.portrait.wasteful;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by li on 2019/1/5.
 */
public class WasteMap implements MapFunction<String, WasteInfo>{



    @Override
    public WasteInfo map(String s) throws Exception {


        if(StringUtils.isBlank(s)){
            return null;
        }

        String[] orderinfos = s.split(",");
        String id= orderinfos[0];
        String productId = orderinfos[1];
        String productTypeId = orderinfos[2];
        String createTime = orderinfos[3];
        String amount = orderinfos[4];// 支付金额
        String payType = orderinfos[5];
        String payTime = orderinfos[6];
        String payStatus = orderinfos[7];
        String couponAmount = orderinfos[8];//
        String totalAmount = orderinfos[9];
        String refundAmount = orderinfos[10];
        String num = orderinfos[11];
        String userId = orderinfos[12];


        /**
         * 构造败家的实体类
         */
        WasteInfo wasteInfo = new WasteInfo();
        wasteInfo.setUserId(userId);
        wasteInfo.setCreateTime(createTime);
        wasteInfo.setAmount(amount);
        wasteInfo.setPayType(payType);
        wasteInfo.setPayTime(payTime);
        wasteInfo.setPayStatus(payStatus);
        wasteInfo.setCouponAmount(couponAmount);
        wasteInfo.setTotalAmount(totalAmount);
        wasteInfo.setRefundAmount(refundAmount);
        String groupfield = "baijia==" + userId;// 先根据用户id分组，根据这个用户的所有订单，计算败家指数
        wasteInfo.setGroupField(groupfield);

        /*
            todo 这边没用的？
         */
        List<WasteInfo> list = new ArrayList<WasteInfo>();
        list.add(wasteInfo);


        WasteInfo ret = new WasteInfo();
        ret.setUserId(userId);
        ret.setGroupField(groupfield);
        ret.setList(list);

        return ret;
    }
}
