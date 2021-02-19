package com.datax.portrait.logistic.sex;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.*;

/**
 * 利用逻辑回归，进行性别的预测
 */
public class SexPredictTask {


    public static void main(String[] args) {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<SexPredictInfo> mapresult = text.map(new SexPredictMap());


        DataSet<ArrayList<Double>> reduceresutl = mapresult
                .groupBy("groupField")
                .reduceGroup(new SexPredictReduce());


        try {
            List<ArrayList<Double>> resultList = reduceresutl.collect();

            int groupSize = resultList.size();


            Map<Integer, Double> summap = new TreeMap<Integer, Double>(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1.compareTo(o2);
                }
            });


            for (ArrayList<Double> array : resultList) {

                for (int i = 0; i < array.size(); i++) {
                    double pre = summap.get(i) == null ? 0d : summap.get(i);
                    summap.put(i, pre + array.get(i));
                }
            }

            ArrayList<Double> finalWeight = new ArrayList<Double>();
            for (Map.Entry<Integer, Double> mapEntry : summap.entrySet()) {

                Double sumvalue = mapEntry.getValue();
                double finalvalue = sumvalue / groupSize;
                finalWeight.add(finalvalue);
            }


            /**
             * 预测
             */
            DataSet<String> text2 = env.readTextFile(params.get("input2"));
            text2.map(new SexPredictSaveMap(finalWeight));


            env.execute("sexPreTask analy");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
