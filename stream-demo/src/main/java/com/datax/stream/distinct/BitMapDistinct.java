package com.datax.stream.distinct;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.roaringbitmap.longlong.Roaring64NavigableMap;


/**
 * 海量数据去重方案(精准去重)
 * <p>
 * 计算每个APP的去重用户数
 */
public class BitMapDistinct {


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
                .aggregate(new BitMapDistinctAggregateFunction());

        distinctDS.print();

        env.execute();
    }


}


class BitMapDistinctAggregateFunction implements AggregateFunction<Tuple2<String, Long>,
        Tuple2<String, Roaring64NavigableMap>, Tuple2<String, Long>> {


    @Override
    public Tuple2<String, Roaring64NavigableMap> createAccumulator() {
        return new Tuple2<>("", new Roaring64NavigableMap());
    }

    @Override
    public Tuple2<String, Roaring64NavigableMap> add(Tuple2<String, Long> value, Tuple2<String, Roaring64NavigableMap> accumulator) {
        accumulator.f0 = value.f0;
        accumulator.f1.add(value.f1);
        return accumulator;
    }


    @Override
    public Tuple2<String, Long> getResult(Tuple2<String, Roaring64NavigableMap> accumulator) {
        // 获取结果
        return new Tuple2<>(accumulator.f0, accumulator.f1.getLongCardinality());
    }

    @Override
    public Tuple2<String, Roaring64NavigableMap> merge(Tuple2<String, Roaring64NavigableMap> a,
                                                       Tuple2<String, Roaring64NavigableMap> b) {
        return null;
    }
}

