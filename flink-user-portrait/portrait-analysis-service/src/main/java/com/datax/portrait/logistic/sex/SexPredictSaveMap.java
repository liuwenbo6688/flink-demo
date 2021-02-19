package com.datax.portrait.logistic.sex;

import com.datax.portrait.logistic.Logistic;
import com.datax.util.HbaseUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;

/**
 * 使用测试数据集进行预测
 */
public class SexPredictSaveMap implements MapFunction<String, SexPredictInfo> {
    private ArrayList<Double> weights = null;

    public SexPredictSaveMap(ArrayList<Double> weights) {
        this.weights = weights;
    }

    @Override
    public SexPredictInfo map(String s) throws Exception {


        String[] temps = s.split("\t");


        // 清洗以及归一化
        int userId = Integer.valueOf(temps[0]);
        long orderNum = Long.valueOf(temps[1]); //订单的总数
        long orderFrequency = Long.valueOf(temps[4]); //隔多少天下单
        int manClothes = Integer.valueOf(temps[5]); //浏览男装次数
        int womenClothes = Integer.valueOf(temps[6]); //浏览女装的次数
        int childClothes = Integer.valueOf(temps[7]); //浏览小孩衣服的次数
        int oldmanClothes = Integer.valueOf(temps[8]); //浏览老人的衣服的次数
        double avgAmount = Double.valueOf(temps[9]); //订单平均金额
        int productTimes = Integer.valueOf(temps[10]); //每天浏览商品数

        ArrayList<String> as = new ArrayList();
        as.add(orderNum + "");
        as.add(orderFrequency + "");
        as.add(manClothes + "");

        as.add(womenClothes + "");
        as.add(childClothes + "");
        as.add(oldmanClothes + "");

        as.add(avgAmount + "");
        as.add(productTimes + "");

        String sexFlag = Logistic.classifyVector(as, weights);
        String sexString = sexFlag == "0" ? "男" : "女";

        String tableName = "user_flag_info";
        String rowkey = userId + "";
        String family = "base_info";
        String colum = "sex";
        HbaseUtils.putdata(tableName, rowkey, family, colum, sexString);

        return null;
    }
}
