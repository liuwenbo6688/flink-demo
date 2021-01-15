package com.datax.dataset.join;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 *  CoGroup
 *
 */
public class TestCoGroup {


    public static void main(String[] args) throws Exception{

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //tuple2<用户id，用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"zs"));
        data1.add(new Tuple2<>(1,"zs"));
        data1.add(new Tuple2<>(1,"zs"));
        data1.add(new Tuple2<>(2,"ls"));
        data1.add(new Tuple2<>(3,"ww"));


        //tuple2<用户id，用户所在城市>
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"beijing"));
        data2.add(new Tuple2<>(2,"shanghai"));
        data2.add(new Tuple2<>(4,"guangzhou"));


        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);



        text1.coGroup(text2)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Long>() {

                    @Override
                    public void coGroup(Iterable<Tuple2<Integer, String>> first,
                                        Iterable<Tuple2<Integer, String>> second,
                                        Collector<Long> out) throws Exception {


                        String text = "";
                        for(Tuple2<Integer, String> s :first){
                            text += s;

                        }

                        text +="    ";
                        for(Tuple2<Integer, String> s :second){
                            text += s;

                        }
                        System.out.println(text);


                        out.collect(1l);
                    }
                }).print();
    }
}
