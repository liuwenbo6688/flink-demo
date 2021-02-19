package com.datax.portrait.tfidf.keyword;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * 跑 年度，季度，月度的商品关键词
 */
public class KeyWordTask {


    public static void main(String[] args) {


        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        /**
         * 根据不同的计算周期，传入不同的文件目录
         */
        DataSet<String> input = env.readTextFile(params.get("input"));





        DataSet<KeyWordEntity> ds1 = input.map(new KeywordMap())  // 解析数据每条数据，<用户，商品关键词>
                                            .groupBy("userId")    // 根据用户ID分组
                                            .reduce(new KeywordReduce()) // 对同一个用户的商品关键词进行合并
                                            .map(new KeywordTfIdfMap1()); // 计算tf 和 累加idf中词条出现的文档数

        // 就是为了计算总数
        DataSet<KeyWordEntity> ds2 = ds1.reduce(new KeyWordCountReduce());


        try {
            // 取回 总数
            Long totalDoucment = ds2.collect().get(0).getTotalDocumet();

            // mapresult2才有计算过得tf
            DataSet<KeyWordEntity> ds3 = ds1.map(new KeywordTfIdfMap2(totalDoucment, 3, "month|quarter|year"));


            ds3.writeAsText("hdfs://youfan/test/month");//hdfs的路径


            env.execute("KeyWordTask analy");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
