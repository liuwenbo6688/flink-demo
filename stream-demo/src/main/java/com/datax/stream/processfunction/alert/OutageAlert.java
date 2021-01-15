package com.datax.stream.processfunction.alert;


import com.google.common.collect.Maps;

import org.apache.flink.api.common.functions.FlatMapFunction;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 *   利用 ProcessFunction 处理宕机告警
 */
public class OutageAlert {
    public static void main(String[] args) throws Exception {

//        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties  = new Properties();
        // todo

        FlinkKafkaConsumer010<MetricEvent> consumer = new FlinkKafkaConsumer010<MetricEvent>(
                "test",
                new MetricSchema(),
                properties);


        env.addSource(consumer)
                .assignTimestampsAndWatermarks(new MetricWatermark())

                .flatMap(new FlatMapFunction<MetricEvent, OutageMetricEvent>() {
                    @Override
                    public void flatMap(MetricEvent metricEvent, Collector<OutageMetricEvent> collector) throws Exception {
                        Map<String, String> tags = metricEvent.getTags();
                        if (tags.containsKey("cluster_name") && tags.containsKey("host_ip")) {
                            OutageMetricEvent outageMetricEvent = OutageMetricEvent.buildFromEvent(metricEvent);
                            if (outageMetricEvent != null) {
                                collector.collect(outageMetricEvent);
                            }
                        }
                    }
                })

                .assignTimestampsAndWatermarks(new OutageMetricWaterMark())

                .keyBy(outageMetricEvent -> outageMetricEvent.getKey())
                .process(new OutageProcessFunction(1000 * 10, 60))
//                .assignTimestampsAndWatermarks(new OutageMetricWaterMark())

                .map(new MapFunction<OutageMetricEvent, AlertEvent>() {                    @Override
                    public AlertEvent map(OutageMetricEvent value) throws Exception {
                        AlertEvent alertEvent = new AlertEvent();
                        alertEvent.setType("outage");
                        alertEvent.setRecover(value.getRecover());
                        alertEvent.setTrigerTime(value.getTimestamp());
                        if (value.getRecover()) {
                            alertEvent.setRecoverTime(value.getRecoverTime());
                        }

                        MetricEvent metricEvent = new MetricEvent();
                        metricEvent.setTimestamp(value.getTimestamp());
                        metricEvent.setName("outage");
                        HashMap<String, Object> fields = Maps.newHashMap();
                        if (value.getMemUsedPercent() != null) {
                            fields.put("mem" + "_" + "usedPercent", value.getMemUsedPercent());
                        }
                        if (value.getLoad5() != null) {
                            fields.put("load5", value.getLoad5());
                        }
                        if (value.getSwapUsedPercent() != null) {
                            fields.put("swap" + "_" + "usedPercent", value.getSwapUsedPercent());
                        }
                        if (value.getCpuUsePercent() != null) {
                            fields.put("cpu" + "_" + "usedPercent", value.getCpuUsePercent());
                        }
                        metricEvent.setFields(fields);
                        HashMap<String, String> tags = Maps.newHashMap();
                        tags.put("cluster_name", value.getClusterName());
                        tags.put("host_ip", value.getHostIp());
                        metricEvent.setTags(tags);

                        alertEvent.setMetricEvent(metricEvent);

                        return alertEvent;
                    }
                })
                .print();

        env.execute("machine outage alert");
    }
}