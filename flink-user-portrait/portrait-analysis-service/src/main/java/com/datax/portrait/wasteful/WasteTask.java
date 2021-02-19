package com.datax.portrait.wasteful;


import com.datax.util.DateUtils;
import com.datax.util.HbaseUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 败家指数 = 支付金额平均值*0.3、 最大支付金额*0.3、 下单频率*0.4
 * <p>
 * 有时间段：近一年，近半年
 */
public class WasteTask {


    public static void main(String[] args) {


        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        /**
         * 近1年的订单数据
         */
        DataSet<String> text = env.readTextFile(params.get("input"));

        /**
         * map
         */
        DataSet<WasteInfo> mapDataSet = text.map(new WasteMap());


        /**
         * reduce
         */
        DataSet<WasteInfo> reduceDataSet = mapDataSet
                .groupBy("groupField")
                .reduce(new WasteReduce());


        try {

            /**
             * todo
             * 这地方有点扯淡了，性能扛不住，每个用户的订单数据都收集回客户端
             */
            List<WasteInfo> resultList = reduceDataSet.collect();

            for (WasteInfo wasteInfo : resultList) {

                String userId = wasteInfo.getUserId();
                List<WasteInfo> list = wasteInfo.getList(); // 用户最近一年的所有消费数据


                /**
                 * 按订单时间排序一下
                 */
                Collections.sort(list, new Comparator<WasteInfo>() {
                    @Override
                    public int compare(WasteInfo o1, WasteInfo o2) {
                        String timeO1 = o1.getCreateTime();
                        String timeO2 = o2.getCreateTime();
                        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmmss");

                        Date now = new Date();
                        Date time1 = now;
                        Date time2 = now;

                        try {
                            time1 = dateFormat.parse(timeO1);
                            time2 = dateFormat.parse(timeO2);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return time1.compareTo(time2);
                    }
                });


                WasteInfo before = null;
                Map<Integer, Integer> frequencyMap = new HashMap<Integer, Integer>();
                double maxAmount = 0d;
                double sum = 0d;



                for (WasteInfo wasteInfoInner : list) {
                    if (before == null) {
                        before = wasteInfoInner;
                        continue;
                    }

                    //计算购买的频率t
                    String beforeTime = before.getCreateTime();
                    String endTime = wasteInfoInner.getCreateTime();

                    /**
                     *  获取前后两个订单的时间间隔
                     */
                    int intervalDays = DateUtils
                            .getIntervalDaysBetween(beforeTime, endTime, "yyyyMMdd hhmmss");

                    int brefore = frequencyMap.get(intervalDays) == null ? 0 : frequencyMap.get(intervalDays);
                    frequencyMap.put(intervalDays, brefore + 1);

                    //计算最大金额
                    String totalAmountString = wasteInfoInner.getTotalAmount();
                    Double totalamout = Double.valueOf(totalAmountString);
                    if (totalamout > maxAmount) {
                        maxAmount = totalamout;
                    }

                    //计算平均值
                    sum += totalamout;

                    before = wasteInfoInner;
                }
                // 平均金额
                double avgAmount = sum / list.size();


                // 下单频率
                int totalDay = 0;
                for (Map.Entry<Integer, Integer> entry : frequencyMap.entrySet()) {
                    Integer frequency = entry.getKey();// 购买频率（天）
                    Integer count = entry.getValue();// // 购买频率的次数
                    totalDay += frequency * count;
                }
                int avgDay = totalDay / list.size(); //平均天数


                // 败家指数 = 支付金额平均值*0.3、最大支付金额*0.3、下单频率*0.4


                // 下单平率30分 （0-5 100 5-10 90 10-30 70 30-60 60 60-80 40 80-100 20 100以上的 10）

                //  支付金额平均值30分（0-20 5 20-60 10 60-100 20 100-150 30 150-200 40 200-250 60 250-350 70 350-450 80 450-600 90 600以上 100  ）
                int avgAmountScore = 0;
                if (avgAmount >= 0 && avgAmount < 20) {
                    avgAmountScore = 5;
                } else if (avgAmount >= 20 && avgAmount < 60) {
                    avgAmountScore = 10;
                } else if (avgAmount >= 60 && avgAmount < 100) {
                    avgAmountScore = 20;
                } else if (avgAmount >= 100 && avgAmount < 150) {
                    avgAmountScore = 30;
                } else if (avgAmount >= 150 && avgAmount < 200) {
                    avgAmountScore = 40;
                } else if (avgAmount >= 200 && avgAmount < 250) {
                    avgAmountScore = 60;
                } else if (avgAmount >= 250 && avgAmount < 350) {
                    avgAmountScore = 70;
                } else if (avgAmount >= 350 && avgAmount < 450) {
                    avgAmountScore = 80;
                } else if (avgAmount >= 450 && avgAmount < 600) {
                    avgAmountScore = 90;
                } else if (avgAmount >= 600) {
                    avgAmountScore = 100;
                }


                // 最大支付金额30分（0-20 5 20-60 10 60-200 30 200-500 60 500-700 80 700 100）
                int maxAmountScore = 0;
                if (maxAmount >= 0 && maxAmount < 20) {
                    maxAmountScore = 5;
                } else if (maxAmount >= 20 && maxAmount < 60) {
                    maxAmountScore = 10;
                } else if (maxAmount >= 60 && maxAmount < 200) {
                    maxAmountScore = 30;
                } else if (maxAmount >= 200 && maxAmount < 500) {
                    maxAmountScore = 60;
                } else if (maxAmount >= 500 && maxAmount < 700) {
                    maxAmountScore = 80;
                } else if (maxAmount >= 700) {
                    maxAmountScore = 100;
                }


                // 下单平率30分 （0-5 100 5-10 90 10-30 70 30-60 60 60-80 40 80-100 20 100以上的 10）
                int avgDayScore = 0;
                if (avgDay >= 0 && avgDay < 5) {
                    avgDayScore = 100;
                } else if (avgDay >= 5 && avgDay < 10) {
                    avgDayScore = 90;
                } else if (avgDay >= 10 && avgDay < 30) {
                    avgDayScore = 70;
                } else if (avgDay >= 30 && avgDay < 60) {
                    avgDayScore = 60;
                } else if (avgDay >= 60 && avgDay < 80) {
                    avgDayScore = 40;
                } else if (avgDay >= 80 && avgDay < 100) {
                    avgDayScore = 20;
                } else if (avgDay >= 100) {
                    avgDayScore = 10;
                }


                double totalScore = (avgAmountScore / 100) * 30 + (maxAmountScore / 100) * 30 + (avgDayScore / 100) * 40;

                /**
                 * 标签写入 hbase
                 */
                String tableName = "user_flag_info";
                String rowkey = userId;
                String famliy = "base_info";
                String column = "waste_soce";
                HbaseUtils.putdata(tableName, rowkey, famliy, column, totalScore + "");
            }
            env.execute("waste score analy");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
