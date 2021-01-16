package com.datax.table.base;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * sql 批处理
 *
 */
public class BatchSQLDemo {


    public static void main(String[] args) throws Exception {


        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 创建一个TableEnvironment
//        BatchTableEnvironment tEnv = BatchTableEnvironment .create(env);
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        //读取数据源
        DataSet<String> ds1 = env.readTextFile("E:\\github_workspace\\flink-demo\\table-demo\\src\\main\\resources\\user.txt");

        DataSet<User> ds2 = ds1.map(new MapFunction<String, User>() {
            @Override
            public User map(String line) throws Exception {
                String[] split = line.split(",");
                return new User(split[0], split[1]);
            }
        });


        //将ds2注册为表user1
        tEnv.registerDataSet("user1", ds2, "uid,name");

        //查询表数据
        Table table = tEnv.sqlQuery("select * from user1").select("name");

        // table转换为DataSet
        DataSet<String> dataSet = tEnv.toDataSet(table, String.class);

        dataSet.print();
    }

    public static class User {
        public String uid;
        public String name;

        //需要无参数构造方法
        public User() {
        }

        public User(String uid, String name) {
            this.uid = uid;
            this.name = name;
        }
    }
}