package com.datax.portrait.kmeans.usergroup;


import com.datax.portrait.kmeans.Cluster;
import com.datax.portrait.kmeans.KMeansRun;
import com.datax.portrait.kmeans.Point;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


/**
 * 利用 kmeans 算法
 * 根据用户的消费情况数据进行聚类分析
 */
public class UserGroupTask {


    public static void main(String[] args) {


        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));


        DataSet<UserGroupInfo> userGroupByUidDataSet = text
                .map(new UserGroupInfoMap())
                .groupBy("groupField")
                .reduce(new UserGroupInfoReduce());

        DataSet<UserGroupInfo> sourceDataSet = userGroupByUidDataSet.map(new UserGroupQuotaMap());

        DataSet<List<Point>> finalResult = sourceDataSet
                .groupBy("groupField")
                .reduceGroup(new UserGroupKMeansReduce());

        try {

            /**
             *  收集分布式跑出来的一批中心点，再次跑kmeans，求一个最终的结果
             */
            List<List<Point>> resultList = finalResult.collect();
            List<float[]> dataSet = new ArrayList();
            List<Integer> idList = new ArrayList();
            for (List<Point> array : resultList) {
                for (Point point : array) {
                    idList.add(point.getId());
                    dataSet.add(point.getlocalArray());
                }
            }
            KMeansRun kMeansRun = new KMeansRun(6, dataSet, idList);

            Set<Cluster> clusterSet = kMeansRun.run();
            List<Point> finalClutercenter = new ArrayList<Point>();
            int count = 100;
            for (Cluster cluster : clusterSet) {
                Point point = cluster.getCenter();
                point.setId(count++);
                finalClutercenter.add(point);
            }


            /**
             * 计算每个点的中心点，保存hbase
             */
            sourceDataSet.map(new KMeansPredictMap(finalClutercenter));


            env.execute("UserGroupTask analy");

        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
