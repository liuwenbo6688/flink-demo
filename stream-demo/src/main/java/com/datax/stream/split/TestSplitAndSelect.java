package com.datax.stream.split;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class TestSplitAndSelect {


    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> input = env.generateSequence(0, 10);


        // SplitStream拆分为多个流
        SplitStream<Long> splitStream = input.split(new OutputSelector<Long>() {

            @Override
            public Iterable<String> select(Long value) {

                List<String> output = new ArrayList<String>();
                if (value % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }

        });

        //splitStream.print();

        // select搭配split使用，从SplitStream中选择一个或者多个流
        DataStream<Long> even = splitStream.select("even");
        DataStream<Long> odd = splitStream.select("odd");
        DataStream<Long> all = splitStream.select("even", "odd");

        //even.print();

        odd.print();

        //all.print();

        env.execute();
    }
}
