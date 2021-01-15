package com.datax.stream.dataskew;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * keyby 后统计count的案例
 * 使用 LocalKeyBy 的方式
 */
public class LocalKeyByFlatMap extends RichFlatMapFunction<String, Tuple2<String, Long>> implements CheckpointedFunction {


    //Checkpoint 时为了保证 Exactly Once，将 buffer 中的数据保存到该 ListState 中
    private ListState<Tuple2<String, Long>> localPvStatListState;

    //本地 buffer，存放 local 端缓存的 app 的 pv 信息
    private HashMap<String, Long> localPvStat;

    //缓存的数据量大小，即：缓存多少数据再向下游发送
    private int batchSize;

    //计数器，获取当前批次接收的数据量
    private AtomicInteger currentSize;

    LocalKeyByFlatMap(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void flatMap(String in, Collector collector) throws Exception {
        //  将新来的数据添加到 buffer 中
        Long pv = localPvStat.getOrDefault(in, 0L);
        localPvStat.put(in, pv + 1);

        // 如果到达设定的批次，则将 buffer 中的数据发送到下游
        if (currentSize.incrementAndGet() >= batchSize) {
            // 遍历 Buffer 中数据，发送到下游
            for (Map.Entry<String, Long> appIdPv : localPvStat.entrySet()) {
                collector.collect(Tuple2.of(appIdPv.getKey(), appIdPv.getValue()));
            }
            // Buffer 清空，计数器清零
            localPvStat.clear();
            currentSize.set(0);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // 将 buffer 中的数据保存到状态中，来保证 Exactly Once
        localPvStatListState.clear();
        for (Map.Entry<String, Long> appIdPv : localPvStat.entrySet()) {
            localPvStatListState.add(Tuple2.of(appIdPv.getKey(), appIdPv.getValue()));
        }

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        // 从状态中恢复 buffer 中的数据
        localPvStatListState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("localPvStat",
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                        })));

        localPvStat = new HashMap();

        if (context.isRestored()) {

            // 从状态中恢复数据到 localPvStat 中
//            for (Tuple2<String, Long> appIdPv : localPvStatListState.get()) {
//                localPvStat.put(appIdPv.f0, appIdPv.f1);
//            }

            // 从状态中恢复 buffer 中的数据
            for (Tuple2<String, Long> appIdPv : localPvStatListState.get()) {
                long pv = localPvStat.getOrDefault(appIdPv.f0, 0L);
                /**
                 *  如果出现 pv != 0，说明改变了并行度，
                 *  ListState 中的数据会被均匀分发到新的 subtask 中
                 *  所以单个 subtask 恢复的状态中可能包含两个相同的 app 的数据
                 */
                localPvStat.put(appIdPv.f0, pv + appIdPv.f1);
            }

            //  从状态恢复时，默认认为 buffer 中数据量达到了 batchSize，需要向下游发送数据了
            currentSize = new AtomicInteger(batchSize);
        } else {
            currentSize = new AtomicInteger(0);
        }
    }
}


