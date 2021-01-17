package com.datax.stream.operatorState;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 想知道两次事件1之间，一共发生多少次其他事件，分别是什么事件
 * <p>
 * 事件流：1 2 3 4 5 1 3 4 5 6 7 1 4 5 3 9 9 2 1...
 * 输出：
 * (4, 2 3 4 5)
 * (5, 3 4 5 6 7)
 * (6, 4 5 3 9 9 2)
 */
public class TestOperatorStateAndCheckpoint {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 设置checkpoint
        env.enableCheckpointing(60000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // AT_LEAST_ONCE 或者  EXACTLY_ONCE，根据场景选择
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // checkpoint 最小间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);

        // checkpoint的超时时间
        checkpointConfig.setCheckpointTimeout(10000L);

        // 当checkpoint发生异常时，是否失败该task
        // 默认是true
        checkpointConfig.setFailOnCheckpointingErrors(true);

        // 检查点的外部持久化策略
        // DELETE_ON_CANCELLATION 当作业被cancel是，删除检查点，检查点状态仅在作业失败时可用
        // RETAIN_ON_CANCELLATION 当作业取消时保留检查点
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        /**
         *
         *    FsStateBackend
         *    taskmanager的内存中持有正在处理的数据，checkpoint的时候将快照写入文件系统目录中的文件，
         *    文件的路径等元数据会传递给 JobManager，存在JobManager的内存中
         *
         *    大状态，长窗口， 大键/值状态
         *    高可用的情况
         *
         **/

//        StateBackend backend=new FsStateBackend(
//                "hdfs://namenode:40010/flink/checkpoints",
//                false);


        /**
         *  MemoryStateBackend
         *  state保存在taskmanager的内存中，checkpoint存储在jobmanager的内存中
         */
        // 只能测试使用，生产环境不能用
        StateBackend backend = new MemoryStateBackend(10 * 1024 * 1024, false);


        /**
         *   RocksDBStateBackend
         *   本地的数据库，透明存在
         *   增量 checkpoint
         *
         *   超大状态，超长窗口，大键/值状态
         *   高可用的情况
         */
//        StateBackend backend = new RocksDBStateBackend(
//                "hdfs://namenode:40010/flink/checkpoints",
//                true);

        env.setStateBackend(backend);


        /**
         * 配置重启策略
         */
        // 固定延迟
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(3, // number of restart attempts
                        Time.of(10, TimeUnit.SECONDS) // delay
                ));


        // 延迟率
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                2,
                Time.of(1, TimeUnit.HOURS),
                Time.of(10, TimeUnit.SECONDS)
        ));


        DataStream<Long> inputStream = env.fromElements(1L, 2L, 3L, 4L, 5L, 1L, 3L, 4L, 5L, 6L, 7L, 1L, 4L, 5L, 3L, 9L, 9L, 2L, 1L);

        /**
         *
         */
        inputStream.flatMap(new CountWithOperatorState())
                // 并行度修改，会让这个实例看着很怪，就是因为状态是针对一个operator
                .setParallelism(1)
                .print();

        env.execute();

    }
}


class CountWithOperatorState extends RichFlatMapFunction<Long, Tuple2<Integer, String>>
        /**
         * 实现 CheckpointedFunction 接口
         */
        implements CheckpointedFunction {


    /**
     * OperatorState
     * 只支持 ListState 类型
     */
    private transient ListState<Long> checkPointCountList;

    /**
     * 原始状态
     */
    private List<Long> listBufferElements;

    @Override
    public void flatMap(Long value, Collector<Tuple2<Integer, String>> out) throws Exception {
        if (value == 1) {
            // 接收到的值1时就计算并输出
            if (listBufferElements.size() > 0) {
                StringBuffer buffer = new StringBuffer();
                for (Long item : listBufferElements) {
                    buffer.append(item + " ");
                }
                out.collect(Tuple2.of(listBufferElements.size(), buffer.toString()));
                listBufferElements.clear();
            }
        } else {
            listBufferElements.add(value);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkPointCountList.clear();
        for (Long item : listBufferElements) {
            checkPointCountList.add(item);
        }
    }


    /**
     * 初始化
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        /**
         * 初始化状态
         */
        ListStateDescriptor<Long> listStateDescriptor =
                new ListStateDescriptor<Long>("checkPointCountList", TypeInformation.of(new TypeHint<Long>() {
                }));
        /**
         * getListState
         * getUnionListState
         * getBroadcastState
         */
        checkPointCountList = context.getOperatorStateStore().getListState(listStateDescriptor);

        if (context.isRestored()) {
            // isRestored代表是挂了重新启动的
            // 恢复数据到原始状态里
            for (Long element : checkPointCountList.get()) {
                listBufferElements.add(element);
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        listBufferElements = new ArrayList<>();
    }
}
