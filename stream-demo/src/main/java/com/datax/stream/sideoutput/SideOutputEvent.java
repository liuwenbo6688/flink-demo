package com.datax.stream.sideoutput;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;


public class SideOutputEvent {


    private static final OutputTag<MetricEvent> machineTag = new OutputTag<MetricEvent>("machine") {
    };


    private static final OutputTag<MetricEvent> dockerTag = new OutputTag<MetricEvent>("docker") {
    };


    private static final OutputTag<MetricEvent> applicationTag = new OutputTag<MetricEvent>("application") {
    };


    private static final OutputTag<MetricEvent> middlewareTag = new OutputTag<MetricEvent>("middleware") {
    };


    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);


        List data = new ArrayList<MetricEvent>();

        MetricEvent event = new MetricEvent();
        event.getTags().put("type", "machine");
        data.add(event);

        DataStreamSource<MetricEvent> source = env.fromCollection(data);


        SingleOutputStreamOperator<MetricEvent> sideOutputData = source.process(new ProcessFunction<MetricEvent, MetricEvent>() {
            @Override
            public void processElement(MetricEvent metricEvent, Context context, Collector<MetricEvent> collector) throws Exception {
                String type = metricEvent.getTags().get("type");
                switch (type) {
                    case "machine":
                        context.output(machineTag, metricEvent);
                        break;
                    case "docker":
                        context.output(dockerTag, metricEvent);
                        break;
                    case "application":
                        context.output(applicationTag, metricEvent);
                        break;
                    case "middleware":
                        context.output(middlewareTag, metricEvent);
                        break;
                    default:
                        collector.collect(metricEvent);
                }
            }
        });


        DataStream<MetricEvent> machine = sideOutputData.getSideOutput(machineTag);
        DataStream<MetricEvent> docker = sideOutputData.getSideOutput(dockerTag);
        DataStream<MetricEvent> application = sideOutputData.getSideOutput(applicationTag);
        DataStream<MetricEvent> middleware = sideOutputData.getSideOutput(middlewareTag);

        machine.print("--machine--");
        docker.print("--docker--");
        application.print("--application--");
        middleware.print("--middleware--");

        env.execute("output side");
    }


}