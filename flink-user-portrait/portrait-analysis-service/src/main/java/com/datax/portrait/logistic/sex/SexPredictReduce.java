package com.datax.portrait.logistic.sex;


import com.datax.portrait.logistic.CreateDataSet;
import com.datax.portrait.logistic.Logistic;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by li on 2019/1/6.
 */
public class SexPredictReduce implements GroupReduceFunction<SexPredictInfo, ArrayList<Double>> {


    @Override
    public void reduce(Iterable<SexPredictInfo> iterable, Collector<ArrayList<Double>> collector) throws Exception {


        Iterator<SexPredictInfo> iterator = iterable.iterator();
        CreateDataSet trainingSet = new CreateDataSet();


        while (iterator.hasNext()) {

            SexPredictInfo sexPreInfo = iterator.next();


            long orderNum = sexPreInfo.getOrderNum();//订单的总数
            long orderFrequency = sexPreInfo.getOrderFrequency();//隔多少天下单
            int manClothes = sexPreInfo.getManClothes();//浏览男装次数
            int womenClothes = sexPreInfo.getWomenClothes();//浏览女装的次数
            int childClothes = sexPreInfo.getChildClothes();//浏览小孩衣服的次数
            int oldmanClothes = sexPreInfo.getOldmanClothes();//浏览老人的衣服的次数
            double avgAmount = sexPreInfo.getAvgAmount();//订单平均金额
            int productTimes = sexPreInfo.getProductTimes();//每天浏览商品数

            int label = sexPreInfo.getLabel();  // 0男，1女


            ArrayList<String> as = new ArrayList<>();
            as.add(orderNum + "");
            as.add(orderFrequency + "");
            as.add(manClothes + "");

            as.add(womenClothes + "");
            as.add(childClothes + "");
            as.add(oldmanClothes + "");

            as.add(avgAmount + "");
            as.add(productTimes + "");

            trainingSet.data.add(as);
            trainingSet.labels.add(label + "");
        }

        ArrayList<Double> weights = Logistic.gradAscent(trainingSet, trainingSet.labels, 500);

        collector.collect(weights);
    }
}
