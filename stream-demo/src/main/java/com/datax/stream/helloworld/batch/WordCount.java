package com.datax.stream.helloworld.batch;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class WordCount {


    public static void main(String[] args) throws Exception {

        //1、获取命令行参数
        final ParameterTool params = ParameterTool.fromArgs(args);

        //2、 set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = WordCountData.getDefaultTextLineDataSet(env);

        DataSet<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer())
                        .groupBy(0)
                        .sum(1);

        System.out.println("Printing result to stdout. Use --output to specify output path.");
        counts.print();


    }

    public static final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
        /**
         * 累加器
         */
        private IntCounter numLines = new IntCounter();

        @Override
        public void open(Configuration parameters) throws Exception {
            /**
             * 注册累加器
             */
            getRuntimeContext().addAccumulator("num-lines", this.numLines);
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            /**
             * 使用累加器
             */
            this.numLines.add(1);

            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    /**
                     * 一进多出
                     */
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

}