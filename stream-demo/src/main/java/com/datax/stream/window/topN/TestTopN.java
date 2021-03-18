package com.datax.stream.window.topN;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;

import java.util.*;

/**
 * 下单金额 TopN
 */
public class TestTopN {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010("test", new SimpleStringSchema(), properties);
        //从最早开始消费
        consumer.setStartFromEarliest();
        DataStream<String> stream = env.addSource(consumer);


        DataStream<OrderDetail> orderStream = stream.map(message -> JSON.parseObject(message, OrderDetail.class));
        orderStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<OrderDetail>() {
            private Long currentTimeStamp = 0L;
            //设置允许乱序时间
            private Long maxOutOfOrderness = 5000L;

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentTimeStamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(OrderDetail element, long previousElementTimestamp) {
                return element.getTimeStamp();
            }
        });


        DataStream<OrderDetail> reduce = orderStream
                .keyBy((KeySelector<OrderDetail, Object>) value -> value.getUserId())
                // 定义一个总时间长度为600秒，每20秒向后滑动一次的滑动窗口
                .window(SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(20)))
                .reduce(new ReduceFunction<OrderDetail>() {
                    @Override
                    public OrderDetail reduce(OrderDetail value1, OrderDetail value2) throws Exception {
                        return new OrderDetail(
                                value1.getUserId(),
                                value1.getItemId(),
                                value1.getCiteName(),
                                value1.getPrice() + value2.getPrice(),
                                value1.getTimeStamp()
                        );
                    }
                });


        // 订单数据会按照用户维度每隔 20 秒进行一次计算，并且通过 windowAll 函数将所有的数据汇聚到一个窗口
        SingleOutputStreamOperator<Tuple2<Double, OrderDetail>> process = reduce
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(20)))
                .process(new ProcessAllWindowFunction<OrderDetail, Tuple2<Double, OrderDetail>, TimeWindow>() {
                             @Override
                             public void process(Context context, Iterable<OrderDetail> elements, Collector<Tuple2<Double, OrderDetail>> out) throws Exception {
                                 TreeMap<Double, OrderDetail> treeMap = new TreeMap<Double, OrderDetail>(new Comparator<Double>() {
                                     @Override
                                     public int compare(Double x, Double y) {
                                         return (x < y) ? -1 : 1;
                                     }
                                 });
                                 Iterator<OrderDetail> iterator = elements.iterator();
                                 if (iterator.hasNext()) {
                                     treeMap.put(iterator.next().getPrice(), iterator.next());
                                     if (treeMap.size() > 10) {
                                         treeMap.pollLastEntry();
                                     }
                                 }
                                 for (Map.Entry<Double, OrderDetail> entry : treeMap.entrySet()) {
                                     out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                                 }
                             }
                         }
                );


        process.print();

        env.execute();

    }


}


class OrderDetail {

    public OrderDetail(Long userId, Long itemId, String citeName, Double price, Long timeStamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.citeName = citeName;
        this.price = price;
        this.timeStamp = timeStamp;
    }

    // 下单用户id
    private Long userId;
    // 商品id
    private Long itemId;
    // 用户所在城市
    private String citeName;
    // 订单金额
    private Double price;
    // 下单时间
    private Long timeStamp;


    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public String getCiteName() {
        return citeName;
    }

    public void setCiteName(String citeName) {
        this.citeName = citeName;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }
}