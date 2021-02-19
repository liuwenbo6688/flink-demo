package com.datax.portrait.kmeans.usergroup;

import com.datax.portrait.kmeans.Cluster;
import com.datax.portrait.kmeans.KMeansRun;
import com.datax.portrait.kmeans.Point;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * 用 kmeans 分批计算 中心点
 */
public class UserGroupKMeansReduce implements GroupReduceFunction<UserGroupInfo, List<Point>> {


    @Override
    public void reduce(Iterable<UserGroupInfo> iterable, Collector<List<Point>> collector) throws Exception {

        Iterator<UserGroupInfo> iterator = iterable.iterator();

        List<float[]> dataSet = new ArrayList();

        List<Integer> idList = new ArrayList();

        while (iterator.hasNext()) {

            UserGroupInfo userGroupInfo = iterator.next();


            float[] f = new float[] {
//                    Float.valueOf(userGroupInfo.getUserId() + ""), // 第一行是id, 不参与计算的
                    Float.valueOf(userGroupInfo.getAvgAmount() + ""),
                    Float.valueOf(userGroupInfo.getMaxAmount() + ""),
                    Float.valueOf(userGroupInfo.getDays()),
                    Float.valueOf(userGroupInfo.getBuyType1()),
                    Float.valueOf(userGroupInfo.getBuyType2()),
                    Float.valueOf(userGroupInfo.getBuyType3()),
                    Float.valueOf(userGroupInfo.getBuyTime1()),
                    Float.valueOf(userGroupInfo.getBuyTime2()),
                    Float.valueOf(userGroupInfo.getBuyTime3()),
                    Float.valueOf(userGroupInfo.getBuyTime4()) };

            idList.add(Integer.valueOf(userGroupInfo.getUserId() + ""));

            dataSet.add(f);
        }

        /**
         *  调用kmeans算法进行计算
         */
        KMeansRun kMeansRun = new KMeansRun(6, dataSet, idList);
        Set<Cluster> clusterSet = kMeansRun.run();


        List<Point> arrayList = new ArrayList();
        for (Cluster cluster : clusterSet) {
            arrayList.add(cluster.getCenter());
        }

        collector.collect(arrayList);
    }



}
