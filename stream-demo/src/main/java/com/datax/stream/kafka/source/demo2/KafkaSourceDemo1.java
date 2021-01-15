package com.datax.stream.kafka.source.demo2;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class KafkaSourceDemo1 {

    public static void main(String[] args) throws  Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();

        props.put("auto.offset.reset", "latest");
        props.put("bootstrap.servers", "10.16.74.40:9092");
        props.put("group.id", "info-1111");
        props.put("flink.partition-discovery.interval-millis", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("fetch.message.max.bytes", "10485760");
        props.put("max.partition.fetch.byte", "10485760");


        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>("test2",
                new SimpleStringSchema(), props);


        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.print("---------------------");

        env.execute("--");

    }
}
