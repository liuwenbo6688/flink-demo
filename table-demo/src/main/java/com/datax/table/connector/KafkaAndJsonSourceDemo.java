package com.datax.table.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * Flink Sql读 kafka 数据源
 *
 *
 */
public class KafkaAndJsonSourceDemo {


    private final static String SOURCE_TOPIC = "test";
    private final static String SINK_TOPIC = "sink";
    private final static String ZOOKEEPER_CONNECT = "master:2181";
    private final static String GROUP_ID = "group1";
    private final static String METADATA_BROKER_LIST = "master:9092";



    public static void main(String[] args) throws Exception {


        //获取执行环境
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建一个TableEnvironment
        StreamTableEnvironment sTableEnv = StreamTableEnvironment.getTableEnvironment(sEnv);

        sTableEnv.connect(new Kafka()
                                .version("0.10")
                                .topic(SOURCE_TOPIC)
                                .startFromLatest()
                                .property("group.id", GROUP_ID)
                                .property("bootstrap.servers", METADATA_BROKER_LIST))
                .withFormat(
                        new Json()
                                //字段缺少是否允许失败
                                .failOnMissingField(false)
                                //使用table的schema
                                .deriveSchema())
                // 定义schema：解析字段及类型
                .withSchema(
                        new Schema()
                                .field("userId", Types.LONG())
                                .field("day", Types.STRING())
                                .field("begintime", Types.LONG())
                                .field("endtime", Types.LONG())
                                .field("data", ObjectArrayTypeInfo.getInfoFor(
                                        Row[].class,
                                        Types.ROW(
                                                new String[]{"package", "activetime"},
                                                new TypeInformation[]{Types.STRING(), Types.LONG()}
                                        )

                                )))
                // 指定流 append 模式
                .inAppendMode()
                // 注册成一个表
                .registerTableSource("user1");


        Table result = sTableEnv.sqlQuery("select userId  from user1");

        DataStream<Row> rowDataStream = sTableEnv.toAppendStream(result, Row.class);

        rowDataStream.print();

        sEnv.execute("Flink SQL");

    }
}
