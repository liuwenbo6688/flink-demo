package com.datax.portrait.kmeans.usergroup;

import com.datax.util.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 计算每个用户的具体指标
 *
 * 平均消费金额，最大金额.....
 */
public class UserGroupQuotaMap implements MapFunction<UserGroupInfo, UserGroupInfo> {
    @Override
    public UserGroupInfo map(UserGroupInfo userGroupInfo) throws Exception {


        /**
         *  消费类目:
         *  电子（电脑，手机，电视）
         *  生活家居（衣服、生活用户，床上用品）
         *  生鲜（油，米等等）
         */


        /**
         *  消费时间点 :
         *  上午（7-12），下午（12-7），晚上（7-12），凌晨（0-7）
         */
        List<UserGroupInfo> list = userGroupInfo.getList();


        /**
         *  按创建时间排序
         */
        Collections.sort(list, new Comparator<UserGroupInfo>() {
            @Override
            public int compare(UserGroupInfo o1, UserGroupInfo o2) {
                String timeo1 = o1.getCreateTime();
                String timeo2 = o2.getCreateTime();
                DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmmss");
                Date datenow = new Date();
                Date time1 = datenow;
                Date time2 = datenow;
                try {
                    time1 = dateFormat.parse(timeo1);
                    time2 = dateFormat.parse(timeo2);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return time1.compareTo(time2);
            }
        });


        double totalAmount = 0l;//总金额
        double maxAmount = Double.MIN_VALUE;//最大金额

        Map<Integer, Integer> frequencyMap = new HashMap();//消费频次
        UserGroupInfo userGroupInfoBefore = null;

        Map<String, Long> productTypeMap = new HashMap();//商品类别map
        productTypeMap.put("1", 0l);
        productTypeMap.put("2", 0l);
        productTypeMap.put("3", 0l);

        Map<Integer, Long> timeMap = new HashMap();//时间的map
        timeMap.put(1, 0l);
        timeMap.put(2, 0l);
        timeMap.put(3, 0l);
        timeMap.put(4, 0l);


        for (UserGroupInfo userGrInfo : list) {
            double totalAmountDouble = Double.valueOf(userGrInfo.getTotalAmount());
            totalAmount += totalAmountDouble;

            if (totalAmountDouble > maxAmount) { // 记录最大金额
                maxAmount = totalAmountDouble;
            }

            if (userGroupInfoBefore == null) {
                userGroupInfoBefore = userGrInfo;
                continue;
            }


            /**
             *  1. 计算购买的频率  <间隔N天购买，发生次数>
             */
            String beforeTime = userGroupInfoBefore.getCreateTime();
            String endTime = userGrInfo.getCreateTime();
            int days = DateUtils.getIntervalDaysBetween(beforeTime, endTime, "yyyyMMdd HHmmss");
            int brefore = frequencyMap.get(days) == null ? 0 : frequencyMap.get(days);
            frequencyMap.put(days, brefore + 1);



            /**
             *  2. 计算消费类目
             */
            String productType = userGrInfo.getProductTypeId();
//            String bitproductype = ReadProperties.getKey(productType, "productypedic.properties");
            Long pre = productTypeMap.get(productType) == null ? 0l : productTypeMap.get(productType);
            productTypeMap.put(productType, pre + 1);


            /**
             * 3. 时间点，
             * 上午（7-12）: 1
             * 下午（12-7）: 2
             * 晚上（7-12）: 3
             * 凌晨（0-7） : 4
             */
            Integer hour = DateUtils.getHourByDate(userGrInfo.getCreateTime());
            int timeType = -1;
            if (hour >= 7 && hour < 12) {
                timeType = 1;
            } else if (hour >= 12 && hour < 19) {
                timeType = 2;
            } else if (hour >= 19 && hour < 24) {
                timeType = 3;
            } else if (hour >= 0 && hour < 7) {
                timeType = 4;
            }
            Long timesPre = timeMap.get(timeType) == null ? 0l : timeMap.get(timeType);
            timeMap.put(timeType, timesPre);
        }


        int size = list.size();
        double avramout = totalAmount / size; //平均消费金额


        Integer totalDays = 0;
        for (Map.Entry<Integer, Integer> map : frequencyMap.entrySet()) {
            totalDays += map.getKey() * map.getValue();
        }
        int days = totalDays / size;  //消费频次


        UserGroupInfo userGroupInfofinal = new UserGroupInfo();
        userGroupInfofinal.setUserId(userGroupInfo.getUserId());
        userGroupInfofinal.setAvgAmount(avramout);
        userGroupInfofinal.setMaxAmount(maxAmount);
        userGroupInfofinal.setDays(days);

        userGroupInfofinal.setBuyType1(productTypeMap.get("1"));
        userGroupInfofinal.setBuyType2(productTypeMap.get("2"));
        userGroupInfofinal.setBuyType3(productTypeMap.get("3"));

        userGroupInfofinal.setBuyTime1(timeMap.get(1));
        userGroupInfofinal.setBuyTime2(timeMap.get(2));
        userGroupInfofinal.setBuyTime3(timeMap.get(3));
        userGroupInfofinal.setBuyTime4(timeMap.get(4));

        /**
         * 随机分配到100个组中
         */
        userGroupInfofinal.setGroupField("usergrouykmean=====" + new Random().nextInt(100));

        return userGroupInfofinal;
    }
}
