package com.datax.stream.async;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExecutorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Example to illustrates how to use {@link AsyncFunction}.
 * <p>
 * 异步 IO
 */
public class AsyncIOExample {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncIOExample.class);

    private static final String ORDERED = "ordered";

    /**
     * A checkpointed source.
     */
    private static class SimpleSource implements SourceFunction<Integer>, ListCheckpointed<Integer> {
        private static final long serialVersionUID = 1L;

        private volatile boolean isRunning = true;
        private int counter = 0;

        private int start = 0;

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(start);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer i : state) {
                this.start = i;
            }
        }

        public SimpleSource(int maxNum) {
            this.counter = maxNum;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while ((start < counter || counter == -1) && isRunning) {

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(start);
                    ++start;

                    // loop back to 0
                    if (start == Integer.MAX_VALUE) {
                        start = 0;
                    }
                }

                Thread.sleep(10L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }


    private static class SampleAsyncFunction extends RichAsyncFunction<Integer, String> {
        private static final long serialVersionUID = 2098635244857937717L;

        private transient ExecutorService executorService;

        private final long sleepFactor;
        private final float failRatio;

        private final long shutdownWaitTS;

        SampleAsyncFunction(long sleepFactor, float failRatio, long shutdownWaitTS) {
            this.sleepFactor = sleepFactor;
            this.failRatio = failRatio;
            this.shutdownWaitTS = shutdownWaitTS;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            executorService = Executors.newFixedThreadPool(30);
        }

        @Override
        public void close() throws Exception {
            super.close();
            ExecutorUtils.gracefulShutdown(shutdownWaitTS, TimeUnit.MILLISECONDS, executorService);
        }

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<String> resultFuture) {

            /**
             * 这个方法里不要做阻塞的操作，要全部异步操作
             */
            executorService.submit(() -> {
                // 模拟外部异步查询
                // wait for while to simulate async operation here
                long sleep = (long) (ThreadLocalRandom.current().nextFloat() * sleepFactor);
                try {
                    Thread.sleep(sleep);

                    if (ThreadLocalRandom.current().nextFloat() < failRatio) {
                        // 模拟异步请求发生异常
                        resultFuture.completeExceptionally(new Exception("wahahahaha..."));
                    } else {
                        // 完成异步请求
                        resultFuture.complete(
                                Collections.singletonList("key-" + (input % 10)));
                    }
                } catch (InterruptedException e) {
                    resultFuture.complete(new ArrayList<>(0));
                }
            });

        }
    }


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        final String mode = "ordered";

        DataStream<Integer> inputStream = env
                .addSource(new SimpleSource(100000));

        AsyncFunction<Integer, String> function =
                new SampleAsyncFunction(100, 0.001f, 20000);

        DataStream<String> result;
        if (ORDERED.equals(mode)) {
            // 有序，消息接收和的发送到下游的顺序相同
            result = AsyncDataStream.orderedWait(inputStream,
                    function, // AsyncFunction
                    10000L,
                    TimeUnit.MILLISECONDS,
                    20)
                    .setParallelism(1);
        } else {
            /**
             * 无序
             * 对于 ProcessingTime 完全无序
             * 对于 EventTime ，两个watermark之间的数据是无序的
             */
            result = AsyncDataStream.unorderedWait(inputStream,
                    function, // AsyncFunction
                    10000L,
                    TimeUnit.MILLISECONDS,
                    20).setParallelism(1);
        }


        // 求 word count
        result.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            private static final long serialVersionUID = -938116068682344455L;

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(new Tuple2<>(value, 1));
            }
        })
                .keyBy(0)
                .sum(1)
                .print();


        env.execute("Async IO Example");
    }
}