package com.datax.stream.keyedState;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * keyed  state
 * <p>
 * 相邻3个value的平均值
 */
public class CountWithKeyedState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {

        Tuple2<Long, Long> currentSum = sum.value();

        if (null == currentSum) {
            // <次数，总数>
            currentSum = Tuple2.of(0L, 0L);
        }

        currentSum.f0 += 1;

        currentSum.f1 += value.f1;

        sum.update(currentSum);

        if (currentSum.f0 >= 3) {// 达到3个元素的时候，计算一下平均值，然后清空状态值
            out.collect(Tuple2.of(value.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        /**
         * keyed state 可以设置TTL
         * 按 processing time 时间过期
         */

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(100))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        /**
         * 注意这里仅仅用了状态，但是没有利用状态来容错
         */
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "avgState",
                        // 类型提示
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                        })
                );

        descriptor.enableTimeToLive(ttlConfig);

        sum = getRuntimeContext().getState(descriptor);
    }
}
