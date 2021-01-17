package com.datax.stream.flatmap;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 */
public class TestFilter {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Long> input = env.generateSequence(-5, 5);

        input.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                // value > 0 的会继续往下走
                return value > 0;
            }
        }).print();

        env.execute();
    }
}
