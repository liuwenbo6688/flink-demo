package com.datax.table.sql;

import com.datax.table.table.api.TableAPIDemo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * sql 测试
 *
 *
 */
public class SQLDemo {


    public static void main(String[] args) throws Exception {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


//        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        BatchTableEnvironment tEnv =  TableEnvironment.getTableEnvironment(env);


        //读取数据源
        DataSet<String> ds = env.readTextFile("E:\\github_workspace\\flink-demo\\table-demo\\src\\main\\resources\\order.txt");

        DataSet<TableAPIDemo.Order> ds2 = ds.map(new MapFunction<String, TableAPIDemo.Order>() {
            @Override
            public TableAPIDemo.Order map(String line) throws Exception {
                String[] split = line.split(",");
                return new TableAPIDemo.Order(split[0], split[1], Integer.parseInt(split[2]), Double.parseDouble(split[3]), Integer.parseInt(split[4]), Integer.parseInt(split[5]));
            }
        });

        //将ds2注册为表table
        tEnv.registerDataSet("orders", ds2);

        //扫描注册的 Orders 表
        Table order = tEnv.sqlQuery("select userId,sum(goodsMoney) as allmoney from orders where orderStatus=1  group by userId");


        //table 转换为DataSet
        DataSet<Row> rowDataSet = tEnv.toDataSet(order, Row.class);
        rowDataSet.print();

    }

    public static class Order {
        public String orderNo;
        public String userId;
        public Integer orderStatus;
        public double goodsMoney;
        public Integer payType;
        public Integer payFrom;

        public Order() {
        }

        public Order(String orderNo, String userId, Integer orderStatus, double goodsMoney, Integer payType, Integer payFrom) {
            this.orderNo = orderNo;
            this.userId = userId;
            this.orderStatus = orderStatus;
            this.goodsMoney = goodsMoney;
            this.payType = payType;
            this.payFrom = payFrom;
        }
    }
}
