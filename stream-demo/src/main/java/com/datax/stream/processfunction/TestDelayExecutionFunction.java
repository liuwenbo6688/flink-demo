package com.datax.stream.processfunction;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * ProcessFunction
 *
 *  最底层的api，表达能力最强
 *
 *  延迟15秒触发程序的demo
 *
 */
public class TestDelayExecutionFunction {


    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用 event time
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.16.74.38:9092");
        props.put("zookeeper.connect", "10.16.74.38:2181");
        props.put("group.id", "ods_group_001");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        props.put("flink.partition-discovery.interval-millis", "10000");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<>(
                "user_behavior",
                new SimpleStringSchema(),
                props
        );


        // the source data stream
        DataStream<Order> stream = env
                .addSource(source)
                .map(new MapFunction<String, Order>() {

                    @Override
                    public Order map(String value) throws Exception {
                        Order order = JSON.parseObject(value, Order.class);
                        return order;
                    }
                })
                /*.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Order>() {
                    @Override
                    public long extractAscendingTimestamp(Order element) {
                        return element.time;
                    }
                })*/;

//        stream.print();

        // apply the process function onto a keyed stream
        DataStream<Tuple2<String, List<Order>>> result = stream
                // 作用在keyBy上, 根据orderid分组
                .keyBy(new KeySelector<Order, String>() {
                    @Override
                    public String getKey(Order value) throws Exception {
                        return value.orderId;
                    }
                })
                /**
                 * ProcessFunction 可以看做一个具有 keyed state 和timers访问权限的 FlatMapFunction
                 */
                .process(new CountWithTimeoutFunction());

        result.print();

        env.execute();
    }


    /**
     * The data type stored in the state
     */
    public static class CountWithTimestamp {
        public String key;
        public long count;
        public long lastModified;
    }


    /**
     * 按key计数，如果某个key在30秒之内没有新的数据到来就发出(key, count)
     *
     */
    public static class CountWithTimeoutFunction extends ProcessFunction<Order, Tuple2<String, List<Order>>> {

        /**
         * The state that is maintained by this process function
         * 状态
         */
        private ValueState<CountWithTimestamp> state;
        private ListState<Order> listState;

        private long latency = 15 * 1000;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState( new ValueStateDescriptor<>("myState", CountWithTimestamp.class) );
            listState = getRuntimeContext().getListState( new ListStateDescriptor<>("listState", Order.class) );
        }

        @Override
        public void processElement(Order order, Context ctx, Collector<Tuple2<String, List<Order>>> out)
                throws Exception {

            // retrieve the current count
            CountWithTimestamp current = state.value();
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = order.orderId;
            }



//            current.count++;
//            current.lastModified = ctx.timestamp();

            long time = System.currentTimeMillis() + latency;
            System.out.println("processElement timestamp:" + time);

            current.lastModified = time;

            state.update(current);
            listState.add(order);

            // 15s之后触发，flink的定时器机制, 就是调用 onTimer 方法
//            ctx.timerService().registerEventTimeTimer(current.lastModified + latency);

            // 处理时间 + 15s, 只有ProcessingTime才会自动触发，EventTime必须再来数据，超过水位线
            ctx.timerService().registerProcessingTimeTimer(time);
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, List<Order>>> out)
                throws Exception {

            // get the state for the key that scheduled the timer
            CountWithTimestamp result = state.value();
            System.out.println("onTimer timestamp:   " + timestamp + ", " + result.lastModified);

            // check if this is an outdated timer or the latest timer
            // 如果30秒内没有数据， lastModified没有被修改过，那么 timestamp == result.lastModified + 30000
            if (timestamp == result.lastModified) {   // 如果相等，证明15s之内没有修改
                // emit the state on timeout

                List<Order> orders = new ArrayList<>();
                Iterator<Order> iterator = listState.get().iterator();
                while(iterator.hasNext()) {
                    orders.add(iterator.next());
                }

                listState.clear();

                out.collect(new Tuple2<>(result.key, orders));
            }
        }


    }





    public static class Order implements Serializable  {
        /**
         * 支付订单ID
         */
        public String orderId;
        /**
         * 状态
         */
        public int status;
        /**
         * 支付时间
         */
        public long time;

        public Order() {

        }

        public Order(String orderId, int status, long time) {
            this.orderId = orderId;
            this.status = status;
            this.time = time;
        }

        public static Order of(String orderId, int status, long time) {
            return new Order(orderId, status, time);
        }

        @Override
        public String toString() {
            return "Order{" +
                    "orderId='" + orderId + '\'' +
                    ", status=" + status +
                    ", time=" + time +
                    '}';
        }
    }







}
