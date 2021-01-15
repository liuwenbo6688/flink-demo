package com.datax.stream.dataskew;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 *
 * KeyedProcessFunction 作为ProcessFunction的扩展，在其onTimer()方法中提供对定时器对应key的访问
 */

public class CountProcessFunction extends KeyedProcessFunction<Tuple, CountRecord, CountRecord> {



    private transient ValueState<Long> state;
    @Override
    public void open(Configuration parameters) throws Exception {

        /**
         * 注意这里仅仅用了状态，但是没有利用状态来容错
         */
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>(
                        "count",
                        // 类型提示
                        TypeInformation.of(new TypeHint<Long>() {
                        })
                );

        state = this.getRuntimeContext().getState(descriptor);

        System.out.println("state----" + state);
    }



    @Override
    public void processElement(CountRecord value, Context ctx, Collector<CountRecord> out) throws Exception {

        System.out.println("processElement " + state);


        if(state == null ||  state.value() == null) {
            state.update(0l);
        }

        if ( state.value() == 0) {
            state.update(value.count);
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L * 5);
        } else {

            state.update(state.value() + value.count);
        }


    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<CountRecord> out) throws Exception {

        //这里可以做业务操作，例如每5分钟将统计结果发送出去
//        out.collect(state.value());

        System.out.println("CurrentKey:" + ctx.getCurrentKey());

        System.out.println("state value: " + state.value() );

        //清除状态
        state.clear();

        //注册新的定时器
        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L * 5);

    }
}