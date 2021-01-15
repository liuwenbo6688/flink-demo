package com.datax.phd.main;


import com.datax.phd.function.ConnectedBroadcastProcessFuntion;
import com.datax.phd.model.Config;
import com.datax.phd.model.EvaluatedResult;
import com.datax.phd.model.UserEvent;
import com.datax.phd.schema.ConfigDeserializationSchema;
import com.datax.phd.schema.EvaluatedResultSerializationSchema;
import com.datax.phd.schema.UserEventDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class Launcher {


    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String GROUP_ID = "group.id";
    public static final String RETRIES = "retries";
    public static final String INPUT_EVENT_TOPIC = "input-event-topic";
    public static final String INPUT_CONFIG_TOPIC = "input-config-topic";
    public static final String OUTPUT_TOPIC = "output-topic";

    private static final Logger log = LoggerFactory.getLogger(Launcher.class);


    // broadcast 广播的变量
    // key ： value
    public static final MapStateDescriptor<String, Config> configStateDescriptor =
            new MapStateDescriptor<>("configBroadcastState", // 状态的名称
                    BasicTypeInfo.STRING_TYPE_INFO, // key的类型， String
                    TypeInformation.of(new TypeHint<Config>() {
                    }) //value的类型 Config
            );


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = parameterCheck(args);
        /**
         * 调用这个，就会展示到flink的web界面上
         */
        env.getConfig().setGlobalJobParameters(params);

        // 使用event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * checkpoint
         */
        env.enableCheckpointing(60000L); //一分钟
        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// exactly once
        checkpointConf.setMinPauseBetweenCheckpoints(30000L);
        checkpointConf.setCheckpointTimeout(10000L);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /**
         * StateBackend
         */
//        env.setStateBackend(new FsStateBackend(
//                "hdfs://namenode01.td.com/flink-checkpoints/customer-purchase-behavior-tracker"));

        /**
         * restart策略
         * 最大重试10次，每次间隔30s
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                10, // number of restart attempts
                org.apache.flink.api.common.time.Time.of(30, TimeUnit.SECONDS) // delay
        ));

        /* Kafka consumer */
        Properties consumerProps = new Properties();
        consumerProps.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS));
        consumerProps.setProperty(GROUP_ID, params.get(GROUP_ID));

        // =====================================事件流==================================================
        final FlinkKafkaConsumer010 kafkaUserEventSource = new FlinkKafkaConsumer010<UserEvent>(
                params.get(INPUT_EVENT_TOPIC),
                new UserEventDeserializationSchema(), consumerProps);

        // (userEvent, userId)
        KeyedStream<UserEvent, String> customerUserEventStream = env
                .addSource(kafkaUserEventSource)
                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor(Time.hours(24)))
                .keyBy(new KeySelector<UserEvent, String>() {
                    @Override
                    public String getKey(UserEvent userEvent) throws Exception {
                        return userEvent.getUserId();
                    }
                });
        //customerUserEventStream.print();


        // =====================================配置流==================================================
        final FlinkKafkaConsumer010 kafkaConfigEventSource = new FlinkKafkaConsumer010<Config>(
                params.get(INPUT_CONFIG_TOPIC),
                new ConfigDeserializationSchema(), consumerProps);

        // BroadcastStream
        final BroadcastStream<Config> configBroadcastStream = env
                .addSource(kafkaConfigEventSource)
                .broadcast(configStateDescriptor);

        //连接两个流
        /* Kafka consumer */
        Properties producerProps = new Properties();
        producerProps.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS));
        producerProps.setProperty(RETRIES, "3");

        final FlinkKafkaProducer010 kafkaProducer = new FlinkKafkaProducer010<EvaluatedResult>(
                params.get(OUTPUT_TOPIC),
                new EvaluatedResultSerializationSchema(),
                producerProps);

        /* at_ least_once 设置 */
        kafkaProducer.setLogFailuresOnly(false);
        kafkaProducer.setFlushOnCheckpoint(true);


        // =======================================
        DataStream<EvaluatedResult> connectedStream = customerUserEventStream
                .connect(configBroadcastStream)  // 两个流做一个 connect，一定要用常规的事件流去connect配置流

                // 最核心方法 ConnectedBroadcastProcessFuntion
                .process(new ConnectedBroadcastProcessFuntion());

        connectedStream.addSink(kafkaProducer);

        env.execute("UserPurchaseBehaviorTracker");

    }

    /**
     * 参数校验
     *
     * @param args
     * @return
     */
    public static ParameterTool parameterCheck(String[] args) {

        //--bootstrap.servers slave03:9092 --group.id test --input-event-topic purchasePathAnalysisInPut --input-config-topic purchasePathAnalysisConf --output-topic purchasePathAnalysisOutPut

        ParameterTool params = ParameterTool.fromArgs(args);

        params.getProperties().list(System.out);

        if (!params.has(BOOTSTRAP_SERVERS)) {
            System.err.println("----------------parameter[bootstrap.servers] is required----------------");
            System.exit(-1);
        }
        if (!params.has(GROUP_ID)) {
            System.err.println("----------------parameter[group.id] is required----------------");
            System.exit(-1);
        }
        if (!params.has(INPUT_EVENT_TOPIC)) {
            System.err.println("----------------parameter[input-event-topic] is required----------------");
            System.exit(-1);
        }
        if (!params.has(INPUT_CONFIG_TOPIC)) {
            System.err.println("----------------parameter[input-config-topic] is required----------------");
            System.exit(-1);
        }
        if (!params.has(OUTPUT_TOPIC)) {
            System.err.println("----------------parameter[output-topic] is required----------------");
            System.exit(-1);
        }

        return params;
    }

    private static class CustomWatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserEvent> {

        public CustomWatermarkExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(UserEvent element) {
            //如何获取数据的时间字段
            return element.getEventTime();
        }

    }
}
