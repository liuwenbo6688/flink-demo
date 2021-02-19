package com.datax.portrait.yearbase;

import com.datax.util.MongoUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import java.util.List;


/**
 * 出生年代标签
 *
 * 离线计算
 */
public class YearBaseTask {


    public static void main(String[] args) {


        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        /**
         * Map
         */
        DataSet<YearBase> mapDataSet = text.map(new YearBaseMap());


        /**
         * Reduce
         */
        DataSet<YearBase> reduceDataSet = mapDataSet.groupBy("groupField")
                .reduce(new YearBaseReduce());


        try {
            //collect
            List<YearBase> reusltList = reduceDataSet.collect();

            for (YearBase yearBase : reusltList) {

                String yearType = yearBase.getYearType();
                Long count = yearBase.getCount();

                Document doc = MongoUtils
                        .findOneBy("year_base_statics", "portrait", yearType);

                if (doc == null) { // 没有就新增
                    doc = new Document();
                    doc.put("info", yearType);
                    doc.put("count", count);
                } else {
                    // 有就累加？ 为什么累加？
                    Long countpPre = doc.getLong("count");
                    Long total = countpPre + count;
                    doc.put("count", total);
                }

                MongoUtils.saveOrUpdateMongo("year_base_statics", "portrait", doc);
            }

            env.execute("year base analy");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
