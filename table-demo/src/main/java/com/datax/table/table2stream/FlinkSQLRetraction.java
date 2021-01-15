package com.datax.table.table2stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Properties;


/**
 * flink sql 窗口
 * <p>
 * https://blog.csdn.net/qq_20672231/article/details/84936716
 */
public class FlinkSQLRetraction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        TableConfig tc = new TableConfig();

        StreamTableEnvironment tableEnv = new StreamTableEnvironment(env, tc);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.16.74.38:9092");
        properties.put("zookeeper.connect", "10.16.74.39:2181");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test_group");

        /*
        0001 中通
        0002 中通
         */
        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(),
                properties);

        DataStream<String> stream = env.addSource(myConsumer);

        // order_id 订单id, tms_company 物流公司
        DataStream<Tuple2<String, String>> map = stream.map(new MapFunction<String, Tuple2<String, String>>() {

            private static final long serialVersionUID = 1471936326697828381L;

            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split(" ");
                return new Tuple2<String, String>(split[0], split[1]);
            }
        });

//        map.print(); //打印流数据


        //注册为user表
        tableEnv.registerDataStream("dwd_table", map, "order_id,tms_company");


        /**
         *  撤回的：    (false,中通,1)
         *  新加入的：   (true,中通,2)
         */
        Table sqlQuery = tableEnv.sqlQuery("select " +
                "    tms_company," +
                "    count(distinct order_id) as order_cnt " +
                "from dwd_table " +
                "group by tms_company");


        //Table 转化为 DataStream
        DataStream<Tuple2<Boolean, Row>> appendStream = tableEnv.toRetractStream(sqlQuery, Row.class);
        appendStream.print();


        env.execute("userPv from Kafka");

    }

}
