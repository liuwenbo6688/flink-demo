package com.datax.stream.distinct;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class BloomFilterDistinct {


    /**
     * APP -> 对应的用户ID
     */
    public static final Tuple2<String, String>[] APP_ID_MAPPING = new Tuple2[]{
            Tuple2.of("app1", "1001"),
            Tuple2.of("app2", "1002"),
            Tuple2.of("app1", "1003"),
            Tuple2.of("app2", "1004"),
            Tuple2.of("app1", "1005"),
            Tuple2.of("app2", "1006"),
            Tuple2.of("app1", "1001"),
            Tuple2.of("app2", "1002"),
            Tuple2.of("app1", "1003"),
            Tuple2.of("app2", "1004"),
            Tuple2.of("app1", "1005"),
            Tuple2.of("app2", "1006")
    };


    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Long>> distinctDS = env
                .fromElements(APP_ID_MAPPING)
                .keyBy(0)
                .process(new BloomFilterDistinctFunction());

        distinctDS.print();

        env.execute();
    }


}


/**
 * 海量数据去重方案
 */
class BloomFilterDistinctFunction extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, Long>> {

    private transient ValueState<BloomFilter> bloomState;
    private transient ValueState<Long> countState;


    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {

        BloomFilter bloomFilter = bloomState.value();
        Long skuCount = countState.value();

        if (bloomFilter == null) {
            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 10000000);
//            BloomFilter.create(Funnels.stringFunnel(), 10000000);
        }

        if (skuCount == null) {
            skuCount = 0L;
        }

        if (!bloomFilter.mightContain(value.f1)) {
            bloomFilter.put(value.f1);
            skuCount = skuCount + 1;
        }

        bloomState.update(bloomFilter);
        countState.update(skuCount);
        out.collect(new Tuple2<>(ctx.getCurrentKey().toString(), countState.value()));
    }


    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>(
                        "countState",
                        // 类型提示
                        TypeInformation.of(new TypeHint<Long>() {
                        })
                );
        countState = this.getRuntimeContext().getState(descriptor);


        ValueStateDescriptor<BloomFilter> descriptor2 =
                new ValueStateDescriptor<>(
                        "bloomState",
                        // 类型提示
                        TypeInformation.of(new TypeHint<BloomFilter>() {
                        })
                );
        bloomState = this.getRuntimeContext().getState(descriptor2);


    }
}
