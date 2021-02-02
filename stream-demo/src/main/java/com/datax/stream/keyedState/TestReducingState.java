package com.datax.stream.keyedState;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 */
public class TestReducingState {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Long>> inputStream = env.fromElements(
                Tuple2.of("a", 4L),
                Tuple2.of("a", 3L),//
                Tuple2.of("a", 1L),
                Tuple2.of("b", 2L),
                Tuple2.of("b", 2L),
                Tuple2.of("b", 2L),
                Tuple2.of("a", 2L),//
                Tuple2.of("a", 9L) //
        );

        inputStream
                .keyBy(0)
                .flatMap(new CountWithReducingState())
                .setParallelism(10)
                .print();

        env.execute();
    }
}


/**
 *
 */
class CountWithReducingState extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<Long, Long>> {

    private transient ReducingState<Long> sumByKey;

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {

        sumByKey.add(value.f1);

        System.out.println(value.f0 + "----" + sumByKey.get());
    }

    @Override
    public void open(Configuration parameters) throws Exception {


        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(100))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();


        ReducingStateDescriptor<Long> descriptor =
                new ReducingStateDescriptor<>(
                        "sumByKey",
                        new ReduceFunction<Long>() {
                            @Override
                            public Long reduce(Long value1, Long value2) throws Exception {
                                return value1 + value2;
                            }
                        },
                        TypeInformation.of(new TypeHint<Long>() {
                        })
                );

        descriptor.enableTimeToLive(ttlConfig);

        sumByKey = getRuntimeContext().getReducingState(descriptor);
    }
}