package com.datax.dbus.function;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.datax.dbus.enums.FlowStatusEnum;
import com.datax.dbus.increment.IncrementSyncApp;
import com.datax.dbus.model.Flow;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;


/**
 * 连接FlatMessage和对应付的Flow配置
 *
 * Broadcast变量保存在内存中，是不会保存到rocks这样的backend中的
 */
public class DbusProcessFuntion extends KeyedBroadcastProcessFunction<String, FlatMessage, Flow, Tuple2<FlatMessage, Flow>> {
    @Override
    public void processElement(FlatMessage value,
                               ReadOnlyContext ctx, // 对 Broadcast 只有读权限
                               Collector<Tuple2<FlatMessage, Flow>> out) throws Exception {

        Flow flow = ctx.getBroadcastState(IncrementSyncApp.flowStateDescriptor)
                .get(value.getDatabase() + value.getTable());

        if (null != flow && flow.getStatus() == FlowStatusEnum.FLOWSTATUS_RUNNING.getCode()) {
            out.collect(Tuple2.of(value, flow));
        }
    }

    @Override
    public void processBroadcastElement(Flow flow,
                                        Context ctx,  // 对 Broadcast 有读写权限
                                        Collector<Tuple2<FlatMessage, Flow>> out) throws Exception {
        BroadcastState<String, Flow> state = ctx.getBroadcastState(IncrementSyncApp.flowStateDescriptor);

        // 把广播的流加入到 状态中
        state.put(flow.getDatabaseName() + flow.getTableName(), flow);
    }
}
