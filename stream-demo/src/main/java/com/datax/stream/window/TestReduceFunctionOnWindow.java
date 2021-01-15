package com.datax.stream.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  ReduceFunction
 */
public class TestReduceFunctionOnWindow {


    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, String, Integer>> input = env.fromElements(ENGLISH_TRANSCRIPT);

        //求各班级英语总分
        DataStream<Tuple3<String, String, Integer>> totalPoints = input
                .keyBy(0)     // 按班级分组
                .countWindow(2) // 2个元素作为一个窗口
                .reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1,
                                                                  Tuple3<String, String, Integer> value2) throws Exception {

                        return new Tuple3<>(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                });


        totalPoints.print();

        env.execute();
    }

    /**
     * 英语成绩
     */
    public static final Tuple3[] ENGLISH_TRANSCRIPT = new Tuple3[]{
            Tuple3.of("class1", "张三", 100),
            Tuple3.of("class1", "李四", 78),
            Tuple3.of("class1", "王五", 99),// 不会触发计算，因为没有class1的第四条记录

            Tuple3.of("class2", "赵六", 81),
            Tuple3.of("class2", "钱七", 59),
            Tuple3.of("class2", "马二", 97)// 不会触发计算，因为没有class2的第四条记录
    };
}
