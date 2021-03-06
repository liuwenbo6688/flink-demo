package com.datax.stream.connectAndUnion;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;
import java.util.Properties;

/**
 * 一个订单分成了大订单和小订单，大小订单对应的数据流来自 Kafka 不同的 Topic，
 * 需要在两个数据流中按照订单 Id 进行匹配，这里认为相同订单 id 的两个流的延迟最大为 60s。
 * 大订单和小订单匹配成功后向下游发送，
 * 若 60s 还未匹配成功，意味着当前只有一个流来临，则认为订单异常，需要将数据进行侧流输出。
 */
public class ConnectOrderStream {


    private static String KAFKA_TOPIC = "";

    private static OutputTag<Order> bigOrderTag = new OutputTag<>("bigOrder");
    private static OutputTag<Order> smallOrderTag = new OutputTag<>("smallOrder");

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        Properties properties = new Properties();


        /**
         * 读取大订单数据，读取的是 json 类型的字符串
         */
        FlinkKafkaConsumerBase<String> consumerBigOrder =
                new FlinkKafkaConsumer010<>("big_order_topic_name", new SimpleStringSchema(), properties)
                        .setStartFromGroupOffsets();

        KeyedStream<Order, String> bigOrderStream = env.addSource(consumerBigOrder)
                .uid(KAFKA_TOPIC) // 有状态算子一定要配置 uid
                .filter(Objects::nonNull) // 过滤掉 null 数据
                .map(str -> JSON.parseObject(str, Order.class)) // 将 json 解析为 Order 类
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(60)) {
                            @Override
                            public long extractTimestamp(Order order) {
                                return order.getTime();
                            }
                        }) // 提取 EventTime，分配 WaterMark
                .keyBy(Order::getOrderId); // 按照 订单id 进行 keyBy


        /**
         * 小订单处理逻辑与大订单完全一样
         */
        FlinkKafkaConsumerBase<String> consumerSmallOrder =
                new FlinkKafkaConsumer010<>("small_order_topic_name", new SimpleStringSchema(), properties)
                        .setStartFromGroupOffsets();

        KeyedStream<Order, String> smallOrderStream = env.addSource(consumerSmallOrder)
                .uid(KAFKA_TOPIC)
                .filter(Objects::nonNull)
                .map(str -> JSON.parseObject(str, Order.class))
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(Order order) {
                                return order.getTime();
                            }
                        })
                .keyBy(Order::getOrderId);


        /**
         *  使用connect连接大小订单的流，然后使用 CoProcessFunction 进行数据匹配
         */
        SingleOutputStreamOperator<Tuple2<Order, Order>> resStream = bigOrderStream.connect(smallOrderStream)
                .process(new CoProcessFunction<Order, Order, Tuple2<Order, Order>>() {

                    // 大订单数据先来了，将大订单数据保存在 bigState 中。
                    ValueState<Order> bigState;

                    // 小订单数据先来了，将小订单数据保存在 smallState 中。
                    ValueState<Order> smallState;

                    ValueState<Long> timerState;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        // 初始化状态信息
                        bigState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("bigState", Order.class));

                        smallState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("smallState", Order.class));


                        timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerStateDescriptor",
                                TypeInformation.of(new TypeHint<Long>() {
                                })));
                    }


                    /**
                     * 大订单的处理逻辑
                     */
                    @Override
                    public void processElement1(Order bigOrder, Context ctx,
                                                Collector<Tuple2<Order, Order>> out)
                            throws Exception {
                        // 获取当前 小订单的状态值
                        Order smallOrder = smallState.value();
                        // smallOrder 不为空表示小订单先来了，直接将大小订单拼接发送到下游
                        if (smallOrder != null) {
                            out.collect(Tuple2.of(smallOrder, bigOrder));
                            // 清空小订单对应的 State 信息
                            smallState.clear();

                            // 这里可以将 Timer 清除。因为两个流都到了，没必要再触发 onTimer 了
                            ctx.timerService().deleteEventTimeTimer(timerState.value());
                            timerState.clear();
                        } else {
                            // 小订单还没来，将大订单放到状态中，并注册 1 分钟之后触发的 timerState
                            bigState.update(bigOrder);

                            // 1 分钟后触发定时器，当前的 eventTime + 60s

                            long time = bigOrder.getTime() + 60000;
                            timerState.update(time); // 1 分钟后触发定时器，并将定时器的触发时间保存在 timerState 中
                            ctx.timerService().registerEventTimeTimer(time);
                        }
                    }


                    /**
                     * 小订单的处理逻辑
                     */
                    @Override
                    public void processElement2(Order smallOrder, Context ctx,
                                                Collector<Tuple2<Order, Order>> out) throws Exception {
                        // 这里先省略代码，小订单的处理逻辑与大订单的处理逻辑完全类似

                        Order bigOrder = bigState.value();

                        if (bigOrder != null) { // 表示大订单先到了

                            out.collect(Tuple2.of(smallOrder, bigOrder));
                            // 清空小订单对应的 State 信息
                            smallState.clear();

                            // 这里可以将 Timer 清除。因为两个流都到了，没必要再触发 onTimer 了
                            ctx.timerService().deleteEventTimeTimer(timerState.value());
                            timerState.clear();

                        } else {
                            // 小订单先到了
                            smallState.update(smallOrder);

                            // 1 分钟后触发定时器，当前的 eventTime + 60s
                            long time = bigOrder.getTime() + 60000;
                            timerState.update(time); // 1 分钟后触发定时器，并将定时器的触发时间保存在 timerState 中
                            ctx.timerService().registerEventTimeTimer(time);
                        }


                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx,
                                        Collector<Tuple2<Order, Order>> out) throws Exception {
                        // 定时器触发了，即 1 分钟内没有接收到两个流。
                        // 大订单不为空，则将大订单信息侧流输出
                        if (bigState.value() != null) {
                            ctx.output(bigOrderTag, bigState.value());
                        }
                        // 小订单不为空，则将小订单信息侧流输出
                        if (smallState.value() != null) {
                            ctx.output(smallOrderTag, smallState.value());
                        }

                        bigState.clear();
                        smallState.clear();
                    }


                });


        // 正常匹配到数据的 输出。生产环境肯定是需要通过 Sink 输出到外部系统
        resStream.print();

        // 只有大订单时，没有匹配到 小订单，属于异常数据，需要保存到外部系统，进行特殊处理
        resStream.getSideOutput(bigOrderTag).print();

        // 只有小订单时，没有匹配到 大订单，属于异常数据，需要保存到外部系统，进行特殊处理
        resStream.getSideOutput(smallOrderTag).print();

        env.execute("......");

    }
}


class Order {
    /**
     * 订单发生的时间
     */
    public long time;
    /**
     * 订单 id
     */
    public String orderId;
    /**
     * 用户id
     */
    public String userId;
    /**
     * 商品id
     */
    public int goodsId;
    /**
     * 价格
     */
    public int price;
    /**
     * 城市
     */
    public int cityId;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public int getGoodsId() {
        return goodsId;
    }

    public void setGoodsId(int goodsId) {
        this.goodsId = goodsId;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public int getCityId() {
        return cityId;
    }

    public void setCityId(int cityId) {
        this.cityId = cityId;
    }
}