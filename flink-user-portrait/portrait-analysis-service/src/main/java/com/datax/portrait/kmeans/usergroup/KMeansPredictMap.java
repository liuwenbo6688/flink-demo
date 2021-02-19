package com.datax.portrait.kmeans.usergroup;

import com.alibaba.fastjson.JSONObject;

import com.datax.portrait.kmeans.DistanceCompute;
import com.datax.portrait.kmeans.Point;
import com.datax.util.HbaseUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.List;

/**
 *  最终计算每个点的中心点
 */
public class KMeansPredictMap implements MapFunction<UserGroupInfo, Point> {

    private List<Point> centers;
    private DistanceCompute distanceCompute = new DistanceCompute();

    public KMeansPredictMap(List<Point> centers) {
        this.centers = centers;
    }

    @Override
    public Point map(UserGroupInfo userGroupInfo) throws Exception {


        float[] f = new float[]{
                Float.valueOf(userGroupInfo.getAvgAmount() + ""),
                Float.valueOf(userGroupInfo.getMaxAmount() + ""),
                Float.valueOf(userGroupInfo.getDays()),
                Float.valueOf(userGroupInfo.getBuyType1()),
                Float.valueOf(userGroupInfo.getBuyType2()),
                Float.valueOf(userGroupInfo.getBuyType3()),
                Float.valueOf(userGroupInfo.getBuyTime1()),
                Float.valueOf(userGroupInfo.getBuyTime2()),
                Float.valueOf(userGroupInfo.getBuyTime3()),
                Float.valueOf(userGroupInfo.getBuyTime4())};



        Point self = new Point(Integer.valueOf(userGroupInfo.getUserId()), f);
        float minDistance = Integer.MAX_VALUE;


        for (Point point : centers) {

            float tmpDis = (float) Math.min(distanceCompute.getEuclideanDis(self, point), minDistance);

            if (tmpDis != minDistance) {
                // 更新最近的中心点
                minDistance = tmpDis;
                self.setClusterId(point.getId());
                self.setDist(minDistance);
                self.setClusterPoint(point);
            }
        }

        String tablename = "user_flag_info";
        String rowkey = self.getId() + "";
        String famliyname = "user_group_info";
        String colum = "user_group_info";//用户分群信息
        HbaseUtils.putdata(tablename, rowkey, famliyname, colum, JSONObject.toJSONString(self));

        return self;
    }
}
