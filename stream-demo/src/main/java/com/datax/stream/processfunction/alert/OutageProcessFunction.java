package com.datax.stream.processfunction.alert;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OutageProcessFunction extends KeyedProcessFunction<String, OutageMetricEvent, OutageMetricEvent> {


    private Logger log = LoggerFactory.getLogger(this.getClass());

    private ValueState<OutageMetricEvent> outageMetricState;

    // 机器告警是否恢复
    private ValueState<Boolean> recover;

    //  持续多久没有收到监控数据的时间
    private int delay;

    // alertCountLimit 表示的是告警的次数，如果超多一定的告警次数则会静默
    private int alertCountLimit;

    public OutageProcessFunction(int delay, int alertCountLimit) {
        this.delay = delay;
        this.alertCountLimit = alertCountLimit;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<OutageMetricEvent> outageInfo = TypeInformation.of(new TypeHint<OutageMetricEvent>() {
        });
        TypeInformation<Boolean> recoverInfo = TypeInformation.of(new TypeHint<Boolean>() {
        });
        outageMetricState = getRuntimeContext().getState(new ValueStateDescriptor<>("outage_zhisheng", outageInfo));
        recover = getRuntimeContext().getState(new ValueStateDescriptor<>("recover_zhisheng", recoverInfo));
    }

    @Override
    public void processElement(OutageMetricEvent outageMetricEvent, Context ctx, Collector<OutageMetricEvent> collector) throws Exception {
        OutageMetricEvent current = outageMetricState.value();

        if (current == null) {
            current = new OutageMetricEvent(
                    outageMetricEvent.getClusterName(),
                    outageMetricEvent.getHostIp(),
                    outageMetricEvent.getTimestamp(),
                    outageMetricEvent.getRecover(),
                    System.currentTimeMillis());
        } else {
            if (outageMetricEvent.getLoad5() != null) {
                current.setLoad5(outageMetricEvent.getLoad5());
            }
            if (outageMetricEvent.getCpuUsePercent() != null) {
                current.setCpuUsePercent(outageMetricEvent.getCpuUsePercent());
            }
            if (outageMetricEvent.getMemUsedPercent() != null) {
                current.setMemUsedPercent(outageMetricEvent.getMemUsedPercent());
            }
            if (outageMetricEvent.getSwapUsedPercent() != null) {
                current.setSwapUsedPercent(outageMetricEvent.getSwapUsedPercent());
            }
            current.setSystemTimestamp(System.currentTimeMillis());
        }


        if (recover.value() != null
                && !recover.value()
                && outageMetricEvent.getTimestamp() > current.getTimestamp()) {


            /**
             * 发送一个机器恢复的事件
             */
            OutageMetricEvent recoverEvent = new OutageMetricEvent(
                    outageMetricEvent.getClusterName(),
                    outageMetricEvent.getHostIp(),
                    current.getTimestamp(),
                    true,
                    System.currentTimeMillis());
            recoverEvent.setRecoverTime(ctx.timestamp());// 恢复时间
            log.info("触发宕机恢复事件:{}", recoverEvent);
            collector.collect(recoverEvent);


            current.setCounter(0);
            outageMetricState.update(current);// todo 同样的代码可能执行2次

            // 机器告警已恢复
            recover.update(true);
        }

        current.setTimestamp(outageMetricEvent.getTimestamp());

        // 更新
        outageMetricState.update(current); //todo  同样的代码可能执行2次


        /**
         * 注册一个事件时间的定时器，时间戳是当前的系统时间加上 delay 的时间
         */
        ctx.timerService().registerEventTimeTimer(current.getSystemTimestamp() + delay);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OutageMetricEvent> out) throws Exception {
        OutageMetricEvent result = outageMetricState.value();

        if (result != null
                && timestamp >= result.getSystemTimestamp() + delay
                && System.currentTimeMillis() - result.getTimestamp() >= delay) {

            if (result.getCounter() > alertCountLimit) {
                log.info("宕机告警次数大于:{} :{}", alertCountLimit, result);
                return;
            }

            log.info("触发宕机告警事件:timestamp = {}, result = {}", System.currentTimeMillis(), result);
            result.setRecover(false);
            out.collect(result);

            // 继续注册timer
            ctx.timerService().registerEventTimeTimer(timestamp + delay);

            // 告警次数 + 1
            result.setCounter(result.getCounter() + 1);

            result.setSystemTimestamp(timestamp);

            // 更新状态数据
            outageMetricState.update(result);
            recover.update(false); // 机器还未恢复
        }


    }
}