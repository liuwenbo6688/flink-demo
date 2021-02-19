package com.datax.portrait.carrier;

import com.datax.util.MongoUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import java.util.List;

/**
 * 运营商标签
 *
 *  离线计算
 */
public class CarrierTask {



    public static void main(String[] args) {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        /**
         * map
         */
        DataSet<CarrierInfo> mapDataSet = text.map(new CarrierMap());


        /**
         * reduce
         */
        DataSet<CarrierInfo> reduceDataSet = mapDataSet
                .groupBy("groupField")
                .reduce(new CarrierReduce());



        try {

            List<CarrierInfo> reusltList = reduceDataSet.collect();


            /**
             * 更新标签统计
             */
            for (CarrierInfo carrierInfo : reusltList) {
                String carrier = carrierInfo.getCarrier();
                Long count = carrierInfo.getCount();

                Document doc = MongoUtils.findOneBy("carrier_statics", "portrait", carrier);
                if (doc == null) {
                    doc = new Document();
                    doc.put("info", carrier);
                    doc.put("count", count);
                } else {
                    Long countpre = doc.getLong("count");
                    Long total = countpre + count;
                    doc.put("count", total);
                }
                MongoUtils.saveOrUpdateMongo("carrier_statics", "portrait", doc);
            }

            env.execute("carrier analy");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
