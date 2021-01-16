package com.datax.stream.helloworld.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data
        System.out.println("Executing WindowWordCount example with default input data set.");
        System.out.println("Use --input to specify file input.");

        DataStream<String> text = env.fromElements(WordCountData.WORDS);

        env.getConfig().setGlobalJobParameters(params);


        DataStream<Tuple2<String, Integer>> counts = text
                .flatMap(new WordCount.Tokenizer())
                .keyBy(0)
                /**
                 * 每当某一个key的个数达到5的时候触发计算，计算最近该key最近10个元素的内容
                 */
                .countWindow(10, 5)
                .sum(1);

        System.out.println("Printing result to stdout. Use --output to specify output path.");
        counts.print();


        // execute program
        env.execute("WindowWordCount");
    }
}