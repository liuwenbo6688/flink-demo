package com.datax.table.connector;

import com.datax.table.table2stream.TableToStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * mysqlSink
 *
 *
 */
public class JDBCAppendTableSinkDemo {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();


        // 创建一个TableEnvironment
//        StreamTableEnvironment sTableEnv = StreamTableEnvironment.create(sEnv);

        StreamTableEnvironment sTableEnv =  TableEnvironment.getTableEnvironment(sEnv);


        //读取数据源
        DataStream<String> ds = sEnv.socketTextStream("master", 9999, "\n");

        // 解析数据
        DataStream<TableToStream.WC> ds2 = ds.flatMap(new FlatMapFunction<String, TableToStream.WC>() {
            @Override
            public void flatMap(String line, Collector<TableToStream.WC> out) throws Exception {
                String[] tokens = line.toLowerCase().split("\\W+");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(new TableToStream.WC(token,1));
                    }
                }
            }
        });

        //DataStream转table
        Table table = sTableEnv.fromDataStream(ds2,"word,frequency");

        //注册表,user1
        sTableEnv.registerTable("wc", table);
        //获取表中所有信息
        Table rs = sTableEnv.sqlQuery("select word,frequency from wc ");

        table.printSchema();
        //将表转化成DataStream
        DataStream<Row> dataStream = sTableEnv.toAppendStream(rs, Row.class);
        //打印输出
        dataStream.print();

        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setUsername("root")
                .setPassword("root")
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/djtdb_test")
                .setQuery("INSERT INTO wc (word,frequency) VALUES (?,?)")
                .setParameterTypes(Types.STRING(), Types.INT())
                .build();

        rs.writeToSink(sink);
        sEnv.execute("JDBCAppendTableSinkDemo");
    }
}
