package com.datax.dataset.groupby;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 *
 */
public class TestGroupBy {


    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple4<Long, String, String, Integer>> inputs = env.fromElements(
                Tuple4.of(1L, "zhangsan", "male", 28),
                Tuple4.of(2L, "lisi", "female", 34),
                Tuple4.of(3L, "wangwu", "female", 23),
                Tuple4.of(4L, "zhaoliu", "male", 34),
                Tuple4.of(5L, "maqi", "male", 25)
        );


        /**
         * 按性别分组，组内按年龄升序排列
         */
        inputs.groupBy(2)
                .sortGroup(3, Order.ASCENDING)
                .first(10)
                .print();


        /**
         * 按性别分组，组内按年龄求和
         */
        inputs.groupBy(2)
                .aggregate(Aggregations.SUM,3)
                .print();


    }


}
