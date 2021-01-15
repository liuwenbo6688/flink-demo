package com.datax.stream.keyby;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 *
 */
public class TestKeyBy {
    public static void main(String[] args) throws Exception {

        //统计各班语文成绩最高分是谁
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<Tuple4<String, String, String, Integer>> input = env.fromElements(TRANSCRIPT);
//        System.out.println("-----------"+input.getParallelism());
        //input.print();


        KeyedStream<Tuple4<String, String, String, Integer>, Tuple> keyedStream = input.keyBy("f0");


        // 自定义 KeySelector
/*        KeyedStream<Tuple4<String, String, String, Integer>, String> keyedStream = input.keyBy(
                new KeySelector<Tuple4<String, String, String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple4<String, String, String, Integer> value) throws Exception {
                        return value.f0;
                    }
                });*/



        /*keyedStream.process(new KeyedProcessFunction<Tuple, Tuple4<String, String, String, Integer>, Object>() {
            @Override
            public void processElement(Tuple4<String, String, String, Integer> value, Context ctx, Collector<Object> out) throws Exception {
                System.out.println("CurrentKey:" + ctx.getCurrentKey());
            }
        }).print();*/



        //System.out.println("***********"+keyedStream.getParallelism());

//        System.out.println("---------444444---"+keyedStream.max(3).getParallelism());

        // 没有window的情况下，每来一条数据计算一次，如果来的第一条数据是最大的，后面会一直打印这条数据
        // sum min max minBy maxBy
        keyedStream.max("f3").print(); // 包含最大值，其余保持不变
//        keyedStream.maxBy("f3").print(); // 返回最大值的整个元素

        env.execute();

//        SingleOutputStreamOperator<Tuple4<String,String,String,Integer>> sumed=keyed.min(3);
//
//        //使用了DataStreamUtils就不需要env.execute()
//        Iterator<Tuple4<String,String,String,Integer>> it=DataStreamUtils.collect(sumed);
//
//        while (it.hasNext()){
//            System.out.println(it.next());
//        }

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
