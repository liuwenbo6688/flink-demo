package com.datax.stream.watermark;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;


/**
 * 迟到的元素也以使用侧输出(side output)特性被重定向到另外的一条流中去。
 * 流的返回值必须是SingleOutputStreamOperator，其是DataStream的子类。
 * 通过getSideOutput方法获取延迟数据。可以将延迟数据重定向到其他流或者进行输出。
 *
 *
 * nc -l 9999
 *
 * 10000 a
 * 25000 a
 * 11000 a
 */

public class TumblingEventWindowExample {


    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<String> socketStream = env.socketTextStream("localhost", 9999);

        /**
         *  保存被丢弃的数据
         */
        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late-data") {
        };


        /**
         * 由于getSideOutput方法是SingleOutputStreamOperator子类中的特有方法
         *
         * 不能使用它的父类dataStream
         */
        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = socketStream
                // Time.seconds(3)有序的情况修改为0
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(String element) {
                        long eventTime = Long.parseLong(element.split(" ")[0]);
                        System.out.println(eventTime);
                        return eventTime;
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return Tuple2.of(value.split(" ")[1], 1L);
                    }
                })
                .keyBy(0)
                // 10s的翻滚窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 收集延迟大于2s的数据
                .sideOutputLateData(outputTag)
                //允许2s延迟
                .allowedLateness(Time.seconds(2))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1,
                                                       Tuple2<String, Long> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                });

        // 没有延迟的数据流
        resultStream.print();


        /**
         *  把迟到的数据暂时打印到控制台，实际中可以保存到其他存储介质中
         */
        DataStream<Tuple2<String, Long>> sideOutput = resultStream.getSideOutput(outputTag);
        sideOutput.print();


        env.execute();
    }
}
