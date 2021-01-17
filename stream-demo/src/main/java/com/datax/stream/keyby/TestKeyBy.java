package com.datax.stream.keyby;


import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 *  统计各班语文成绩最高分是谁
 */
public class TestKeyBy {
    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();


        DataStream<Tuple4<String, String, String, Integer>> input = env
                .fromElements(TRANSCRIPT);

        KeyedStream<Tuple4<String, String, String, Integer>, Tuple> keyedStream = input
                .keyBy("f0");


        // 没有window的情况下，每来一条数据计算一次，如果来的第一条数据是最大的，后面会一直打印这条数据
        // sum min max minBy maxBy

        /**
         * 包含最大值，其余保持不变
         * 在这里就是 f0,f1,f2是不变的，只有f3替换成最大值
         */
        keyedStream.max("f3").print();

        /**
         *  返回最大值的整个元素
         */
        //keyedStream.maxBy("f3").print();

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
