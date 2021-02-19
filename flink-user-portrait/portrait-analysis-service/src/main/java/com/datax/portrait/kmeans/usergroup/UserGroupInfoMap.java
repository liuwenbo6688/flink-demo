package com.datax.portrait.kmeans.usergroup;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by li on 2019/1/13.
 */
public class UserGroupInfoMap implements MapFunction<String, UserGroupInfo> {

    @Override
    public UserGroupInfo map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        String[] orderinfos = s.split(",");

        String id = orderinfos[0];
        String productId = orderinfos[1];

        String productTypeId = orderinfos[2];
        String createtime = orderinfos[3];
        String amount = orderinfos[4];
        String paytype = orderinfos[5];
        String paytime = orderinfos[6];
        String payStatus = orderinfos[7];
        String couponAmount = orderinfos[8];
        String totalAmount = orderinfos[9];
        String refundAmount = orderinfos[10];
        String num = orderinfos[11];
        String userId = orderinfos[12];

        UserGroupInfo userGroupInfo = new UserGroupInfo();
        userGroupInfo.setUserId(userId);
        userGroupInfo.setCreateTime(createtime);
        userGroupInfo.setAmount(amount);
        userGroupInfo.setPayType(paytype);
        userGroupInfo.setPayTime(paytime);
        userGroupInfo.setPayStatus(payStatus);
        userGroupInfo.setCouponAmount(couponAmount);
        userGroupInfo.setTotalAmount(totalAmount);
        userGroupInfo.setRefundAmount(refundAmount);
        userGroupInfo.setCount(Long.valueOf(num));
        userGroupInfo.setProductTypeId(productTypeId);
        userGroupInfo.setGroupField(userId + "==userGroupinfo");
        List<UserGroupInfo> list = new ArrayList();
        list.add(userGroupInfo);
        userGroupInfo.setList(list);

        return userGroupInfo;
    }
}
