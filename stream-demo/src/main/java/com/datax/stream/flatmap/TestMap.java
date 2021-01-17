package com.datax.stream.flatmap;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 */
public class TestMap {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * 生成10-15
         */
        DataStream<Long> input = env.generateSequence(10, 15);

        DataStream plusOne = input.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                /**
                 * 1进1出
                 */
                System.out.println("--------------------" + value);
                return value + 10;
            }
        });

        plusOne.print();

        env.execute();
    }
}
