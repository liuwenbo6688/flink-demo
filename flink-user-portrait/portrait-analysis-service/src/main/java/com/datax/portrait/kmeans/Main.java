package com.datax.portrait.kmeans;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Main {

    /**
     * 运行 main 方法
     * @param args
     */
    public static void main(String[] args) {

        ArrayList<float[]> dataSet = new ArrayList<float[]>();


        List<Integer> idList = new ArrayList();
        idList.add(1);
        idList.add(2);
        idList.add(3);
        idList.add(4);
        idList.add(5);
        idList.add(6);
        idList.add(7);
        idList.add(8);
        idList.add(9);
        idList.add(10);
        idList.add(11);
        idList.add(12);
        idList.add(13);

        /**
         * 模拟13条数据
         */
        dataSet.add(new float[]{1, 2, 3});
        dataSet.add(new float[]{3, 3, 3});
        dataSet.add(new float[]{3, 4, 4});
        dataSet.add(new float[]{5, 6, 5});
        dataSet.add(new float[]{8, 9, 6});
        dataSet.add(new float[]{4, 5, 4});
        dataSet.add(new float[]{6, 4, 2});
        dataSet.add(new float[]{3, 9, 7});
        dataSet.add(new float[]{5, 9, 8});
        dataSet.add(new float[]{4, 2, 10});
        dataSet.add(new float[]{1, 9, 12});
        dataSet.add(new float[]{7, 8, 112});
        dataSet.add(new float[]{7, 8, 4});


 
        KMeansRun kRun =new KMeansRun(3, dataSet, idList);
 
        Set<Cluster> clusterSet = kRun.run();


        System.out.println("单次迭代运行次数："+kRun.getIterTimes());

        for (Cluster cluster : clusterSet) {
            System.out.println(cluster);
        }
    }
}
