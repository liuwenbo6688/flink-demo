package com.datax.stream.distinct;

import net.agkn.hll.HLL;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 海量数据去重方案
 * <p>
 * 输入： Tuple2<String,Long> 代表 <SKU, 访问的用户id>
 */


/**
 * 海量数据去重方案(精准去重)
 * <p>
 * 计算每个APP的去重用户数
 */
public class HyperLogLogDistinct {


    /**
     * APP -> 对应的用户ID
     */
    public static final Tuple2[] APP_ID_MAPPING = new Tuple2[]{
            Tuple2.of("app1", 1001L),
            Tuple2.of("app2", 1002L),
            Tuple2.of("app1", 1003L),
            Tuple2.of("app2", 1004L),
            Tuple2.of("app1", 1005L),
            Tuple2.of("app2", 1006L),
            Tuple2.of("app1", 1001L),
            Tuple2.of("app2", 1002L),
            Tuple2.of("app1", 1003L),
            Tuple2.of("app2", 1004L),
            Tuple2.of("app1", 1005L),
            Tuple2.of("app2", 1006L)
    };


    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Long>> input = env.fromElements(APP_ID_MAPPING);


        DataStream<Tuple2<String, Long>> distinctDS = input
                .keyBy(0)
                .countWindow(3) //模拟3个输出一次
                .aggregate(new HyperLogLogDistinctFunction());

        distinctDS.print();

        env.execute();
    }


}


class HyperLogLogDistinctFunction implements AggregateFunction<Tuple2<String, Long>, Tuple2<String, HLL>, Tuple2<String, Long>> {


    @Override
    public Tuple2<String, HLL> createAccumulator() {

        return new Tuple2<>("", new HLL(14, 5));
    }

    @Override
    public Tuple2<String, HLL> add(Tuple2<String, Long> value, Tuple2<String, HLL> accumulator) {

        //value为购买记录 <商品sku, 用户id>
        /**
         *  addRaw 方法用于向 HyperLogLog 中插入元素
         *  如果插入的元素非数值型的，则需要 hash 过后才能插入
         */
        accumulator.f0 = value.f0;
        accumulator.f1.addRaw(value.f1);

        return accumulator;
    }

    @Override
    public Tuple2<String, Long> getResult(Tuple2<String, HLL> accumulator) {
        // accumulator.cardinality() 方法用于计算 HyperLogLog 中元素的基数
        long cardinality = accumulator.f1.cardinality();

        return new Tuple2<>(accumulator.f0, cardinality);
    }


    @Override
    public Tuple2<String, HLL> merge(Tuple2<String, HLL> a, Tuple2<String, HLL> b) {
        a.f1.union(b.f1);
        return a;
    }
}
