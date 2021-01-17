package com.datax.stream.keyby;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * KeyedProcessFunction
 */
public class TestKeyBy1 {
    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<Tuple4<String, String, String, Integer>> input = env.fromElements(TRANSCRIPT);


        // 自定义 KeySelector
        KeyedStream<Tuple4<String, String, String, Integer>, String> keyedStream = input.keyBy(
                new KeySelector<Tuple4<String, String, String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple4<String, String, String, Integer> value) throws Exception {
                        return value.f0;
                    }
                });


        /**
         *  KeyedProcessFunction
         */
        keyedStream
                .process(new KeyedProcessFunction<String, Tuple4<String, String, String, Integer>, String>() {
                    @Override
                    public void processElement(Tuple4<String, String, String, Integer> value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        System.out.println("CurrentKey:" + ctx.getCurrentKey());
                        out.collect(value.f1);
                    }
                })
                .print();


        env.execute();
    }

    public static final Tuple4[] TRANSCRIPT = new Tuple4[]{
            // 班级，姓名，科目，成绩
            Tuple4.of("class1", "张三", "语文", 100),
            Tuple4.of("class1", "李四", "语文", 78),
            Tuple4.of("class1", "王五", "语文", 99),

            Tuple4.of("class2", "赵六", "语文", 81),
            Tuple4.of("class2", "钱七", "语文", 59),
            Tuple4.of("class2", "马二", "语文", 97)
    };
}
