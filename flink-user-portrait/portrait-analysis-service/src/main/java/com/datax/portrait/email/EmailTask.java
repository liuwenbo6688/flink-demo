package com.datax.portrait.email;

import com.datax.util.MongoUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import java.util.List;

/**
 * 邮件标签
 */
public class EmailTask {


    public static void main(String[] args) {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));


        DataSet<EmaiInfo> mapDataSet = text.map(new EmailMap());


        DataSet<EmaiInfo> reduceDataSet = mapDataSet
                .groupBy("groupField")
                .reduce(new EmailReduce());

        try {
            List<EmaiInfo> reusltList = reduceDataSet.collect();
            for (EmaiInfo emaiInfo : reusltList) {
                String emailType = emaiInfo.getEmailType();
                Long count = emaiInfo.getCount();

                Document doc = MongoUtils.findOneBy("email_statics", "portrait", emailType);
                if (doc == null) {
                    doc = new Document();
                    doc.put("info", emailType);
                    doc.put("count", count);
                } else {
                    Long countpre = doc.getLong("count");
                    Long total = countpre + count;
                    doc.put("count", total);
                }
                MongoUtils.saveOrUpdateMongo("email_statics", "portrait", doc);
            }
            env.execute("email analy");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
