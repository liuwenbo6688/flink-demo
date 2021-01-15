package com.datax.stream.operatorState;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


/**
 * 监控数据超过阈值并达到指定的次数后，就进行报警
 */
public class ThresholdWarning {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 开启检查点机制
        env.enableCheckpointing(1000);


        DataStreamSource<Tuple2<String, Long>> tuple2DataStreamSource = env
                .setParallelism(1) // 设置并行度为1
                .fromElements(
                        Tuple2.of("a", 50L),
                        Tuple2.of("a", 80L),
                        Tuple2.of("a", 400L),
                        Tuple2.of("a", 100L),
                        Tuple2.of("a", 200L),
                        Tuple2.of("a", 200L),
                        Tuple2.of("b", 100L),
                        Tuple2.of("b", 200L),
                        Tuple2.of("b", 200L),
                        Tuple2.of("b", 500L),
                        Tuple2.of("b", 600L),
                        Tuple2.of("b", 700L));

        tuple2DataStreamSource
                .flatMap(new ThresholdWarningFunction(100L, 3))
                .printToErr();

        env.execute("Managed Keyed State");

    }

}


class ThresholdWarningFunction extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, List<Tuple2<String, Long>>>>
        implements CheckpointedFunction {

    // 非正常数据
    private List<Tuple2<String, Long>> bufferedData;

    // checkPointedState
    private transient ListState<Tuple2<String, Long>> checkPointedState;

    // 需要监控的阈值
    private Long threshold;

    // 次数
    private Integer numberOfTimes;

    ThresholdWarningFunction(Long threshold, Integer numberOfTimes) {
        this.threshold = threshold;
        this.numberOfTimes = numberOfTimes;
        this.bufferedData = new ArrayList<>();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        // 注意这里获取的是OperatorStateStore
        checkPointedState = context.getOperatorStateStore().
                getListState(new ListStateDescriptor<>("abnormalData",
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                        })));

        // 如果发生重启，则需要从快照中将状态进行恢复
        if (context.isRestored()) {
            for (Tuple2<String, Long> element : checkPointedState.get()) {
                bufferedData.add(element);
            }
        }
    }

    @Override
    public void flatMap(Tuple2<String, Long> value,
                        Collector<Tuple2<String, List<Tuple2<String, Long>>>> out) {
        Long inputValue = value.f1;
        // 超过阈值则进行记录
        if (inputValue >= threshold) {
            bufferedData.add(value);
        }
        // 超过指定次数则输出报警信息
        if (bufferedData.size() >= numberOfTimes) {
            // 顺便输出状态实例的hashcode
            out.collect(Tuple2.of(checkPointedState.hashCode() + " 阈值警报！", bufferedData));
            bufferedData.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 在进行快照时，将数据存储到checkPointedState
        checkPointedState.clear();
        for (Tuple2<String, Long> element : bufferedData) {
            checkPointedState.add(element);
        }
    }
}
