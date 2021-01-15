package com.datax.stream.dataskew;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

/**
 *  Flink  数据倾斜的解决方案
 */
public class DataSkewDemo {


    public static void main(String[] args) throws Exception {

        //统计各班语文成绩最高分是谁
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);


        DataStream<String> sourceStream = env.addSource(new RecordSource());


        DataStream<CountRecord> resultStream = sourceStream
                .map(recordStr -> {
                    Record record = JSON.parseObject(recordStr, Record.class);
                    record.key = record.key + "#" + new Random().nextInt(2);
                    return record;
                })

                //
                .keyBy("key")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new CountAggregate())

                //
                .map(count -> {
                    System.out.println(count.key + "----"+ count.count);
                    String key = count.key.substring(0, count.key.indexOf("#"));

                    return new CountRecord(key, count.count);
                })
                .keyBy("key")
                .process(new CountProcessFunction());

        resultStream.print();

        env.execute();


    }

}
