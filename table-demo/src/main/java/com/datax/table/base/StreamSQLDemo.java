package com.datax.table.base;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.*;

/**
 * sql 流处理 demo
 *
 *
 */
public class StreamSQLDemo {
//    public static void main1(String[] args) {
//        Map<String, TypeInformation<?>>  field = new LinkedHashMap();
//        field.put("uid", Types.STRING());
//        field.put("name", Types.STRING());
//        field.put("id_1", Types.INT());
//        field.put("id_2", Types.LONG());
//        field.put("id_3", Types.BOOLEAN());
//        field.put("id_4", Types.INT());
//        field.put("id_5", Types.STRING());
//        field.put("id_6", Types.BYTE());
//
//
//
//        System.out.println(Arrays.toString(field.keySet().toArray(new String[0])));
//        System.out.println(Arrays.toString(field.values().toArray(new TypeInformation[0])) );
//    }

    public static void main(String[] args) throws Exception {


        //获取执行环境
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();


        // 创建一个TableEnvironment
//        StreamTableEnvironment sTableEnv = StreamTableEnvironment.create(sEnv);
        StreamTableEnvironment sTableEnv = StreamTableEnvironment.getTableEnvironment(sEnv);


        //读取数据源
        DataStream<User> ds = sEnv
                .readTextFile("E:\\github_workspace\\flink-demo\\table-demo\\src\\main\\resources\\user.txt")
                .map(new MapFunction<String, User>() {
                    @Override
                    public User map(String line) throws Exception {
                        String[] split = line.split(",");
                        return new User(split[0], split[1]);
                    }
                });


        /**
         *  1. DataStream 转  Table
         */
        Table table = sTableEnv.fromDataStream(ds);


        /**
         *  2. 注册表 user_info
         */
        sTableEnv.registerTable("user_info", table);

        /**
         *  3. 查询 user_info
         *
         *  xxxxx
         */
        Table rs = sTableEnv.sqlQuery("select uid,name from user_info");


        /**
         *  4. Table 转 DataStream
         *
         *   AppendStream: 理解
         */
        DataStream<Row> dataStream = sTableEnv.toAppendStream(rs, Row.class);
//        DataStream<Row> dataStream = sTableEnv.toRetractStream(rs, Row.class);

        Map<String, TypeInformation<?>> field = new LinkedHashMap<>();
        field.put("uid", Types.STRING());
        field.put("name", Types.STRING());
        field.put("id_1", Types.INT());
        field.put("id_2", Types.INT());

//        field.keySet().toArray()




//        TypeInformation<?>[] types = {Types.STRING(), Types.STRING(), Types.INT(), Types.INT()};
//        String[] fieldNames = {"uid", "name", "id_1", "id_2"};

        DataStream<Row> ds1 = dataStream.map(new MapFunction<Row, Row>() {

            @Override
            public Row map(Row value) throws Exception {


                Row row = new Row(value.getArity() + 2);

                for (int i = 0; i < value.getArity(); i++) {
                    row.setField(i, value.getField(i));
                }


                /**
                 * 模拟查询维度表
                 * id查询
                 */
                row.setField(value.getArity(), 100);
                row.setField(value.getArity() + 1, 3);


                return row;
            }
        }).returns(new RowTypeInfo( field.values().toArray(new TypeInformation[0]), field.keySet().toArray(new String[0])));


        System.out.println(Arrays.toString( field.keySet().toArray(new String[0])  ));
        System.out.println(Arrays.toString(  field.values().toArray(new TypeInformation[0])  ) );


        Table t2 = sTableEnv.fromDataStream(ds1);
        sTableEnv.registerTable("user_info_2", t2);


        /**
         * sssss
         */
        Table t3 = sTableEnv.sqlQuery("select * from user_info_2");

//        t3.printSchema();


        DataStream<Row> ds3 = sTableEnv.toAppendStream(t3, Row.class);
        ds3.print();
//        ds1.print();


//        dataStream.print();
        sEnv.execute("Flink SQL");
    }

    public static class User {
        public String uid;
        public String name;

        // 必须定义一个空的构造函数
        public User() {
        }
        public User(String uid, String name) {
            this.uid = uid;
            this.name = name;
        }

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
