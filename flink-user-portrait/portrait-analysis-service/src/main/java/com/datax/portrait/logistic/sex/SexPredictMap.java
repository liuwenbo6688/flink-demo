package com.datax.portrait.logistic.sex;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.Random;

/**
 *
 */
public class SexPredictMap implements MapFunction<String, SexPredictInfo> {


    @Override
    public SexPredictInfo map(String s) throws Exception {
        String[] temps = s.split("\t");

        Random random = new Random();

        // 清洗以及归一化
        int userId = Integer.valueOf(temps[0]);
        long orderNum = Long.valueOf(temps[1]);//订单的总数
        long orderFrequency = Long.valueOf(temps[4]);//隔多少天下单
        int manClothes = Integer.valueOf(temps[5]);//浏览男装次数
        int womenClothes = Integer.valueOf(temps[6]);//浏览女装的次数
        int childClothes = Integer.valueOf(temps[7]);//浏览小孩衣服的次数
        int oldmanClothes = Integer.valueOf(temps[8]);//浏览老人的衣服的次数
        double avgAmount = Double.valueOf(temps[9]);//订单平均金额
        int productTimes = Integer.valueOf(temps[10]);//每天浏览商品数
        int label = Integer.valueOf(temps[11]);//0男，1女

        String fieldgroup = "sexpre==" + random.nextInt(10);

        SexPredictInfo sexPreInfo = new SexPredictInfo();
        sexPreInfo.setUserId(userId);
        sexPreInfo.setOrderNum(orderNum);
        sexPreInfo.setOrderFrequency(orderFrequency);
        sexPreInfo.setManClothes(manClothes);
        sexPreInfo.setWomenClothes(womenClothes);
        sexPreInfo.setChildClothes(childClothes);
        sexPreInfo.setOldmanClothes(oldmanClothes);
        sexPreInfo.setAvgAmount(avgAmount);
        sexPreInfo.setProductTimes(productTimes);
        sexPreInfo.setLabel(label);
        sexPreInfo.setGroupField(fieldgroup);


        return sexPreInfo;
    }


}
