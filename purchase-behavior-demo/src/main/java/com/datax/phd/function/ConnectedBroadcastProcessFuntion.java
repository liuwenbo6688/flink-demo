package com.datax.phd.function;

import com.alibaba.fastjson.JSON;
import com.datax.phd.model.*;
import com.datax.phd.main.Launcher;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 *
 */
public class ConnectedBroadcastProcessFuntion
            // <事件流的key, 事件流的value, 配置流的value, 最后的输出结果>
        extends KeyedBroadcastProcessFunction<String, UserEvent, Config, EvaluatedResult> {

    private static final Logger log = LoggerFactory.getLogger(ConnectedBroadcastProcessFuntion.class);

    private Config defaultConfig = new Config("APP","2018-01-01",0,3);


    /**
     *
     */
    // (channel, Map<uid, UserEventContainer>)
    private final MapStateDescriptor<String, Map<String, UserEventContainer>> userMapStateDesc =
            new MapStateDescriptor<>(
                    "userEventContainerState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    new MapTypeInfo<>(String.class, UserEventContainer.class));

    @Override
    public void processElement(UserEvent value, ReadOnlyContext ctx, Collector<EvaluatedResult/*输出类型*/> out) throws Exception {
        // value就是事件流
        String userId = value.getUserId();
        String channel = value.getChannel();

        EventType eventType = EventType.valueOf(value.getEventType());

        // 只读取这个channel的配置
        Config config = ctx.getBroadcastState(Launcher.configStateDescriptor).get(channel);
        log.info("Read config: channel=" + channel + ", config=" + config);
        if (Objects.isNull(config)) { // 没有设置就去默认配置
            config = defaultConfig;
        }

        final MapState<String, Map<String, UserEventContainer>> state = getRuntimeContext().getMapState(userMapStateDesc);

        // collect per-user events to the user map state
        // 某一个渠道所有用户的信息
        Map<String, UserEventContainer> userEventContainerMap = state.get(channel);
        if (Objects.isNull(userEventContainerMap)) {
            userEventContainerMap = Maps.newHashMap();
            state.put(channel, userEventContainerMap);
        }
        if (!userEventContainerMap.containsKey(userId)) {
            UserEventContainer container = new UserEventContainer();
            container.setUserId(userId);
            userEventContainerMap.put(userId, container);
        }
        userEventContainerMap.get(userId).getUserEvents().add(value);

        // check whether a user purchase event arrives
        // if true, then compute the purchase path length, and prepare to trigger predefined actions
        if (eventType == EventType.PURCHASE) { // 如果是购买行为
            log.info("Receive a purchase event: " + value);
            Optional<EvaluatedResult> result = compute(config, userEventContainerMap.get(userId));
            result.ifPresent(r -> out.collect(result.get()));

            // clear evaluated user's events
            // 清理掉用户的数据
            state.get(channel).remove(userId);
        }
    }

    // 每一个广播的配置流，都会走进这个方法
    @Override
    public void processBroadcastElement(Config value, Context ctx, Collector<EvaluatedResult> out) throws Exception {
        //value 就是广播的配置流的Config
        String channel = value.getChannel();
        BroadcastState<String, Config> state = ctx.getBroadcastState(Launcher.configStateDescriptor);

        // 看一下配置是新增还是修改了，没什么用处
        final Config oldConfig = ctx.getBroadcastState(Launcher.configStateDescriptor).get(channel);
        if (state.contains(channel)) {
            log.info("Configured channel exists: channel=" + channel);
            log.info("Config detail: oldConfig=" + oldConfig + ", newConfig=" + value);
        } else {
            log.info("Config detail: defaultConfig=" + defaultConfig + ", newConfig=" + value);
        }

        // 更新 state变量里的配置信息
        // update config value for configKey
        state.put(channel, value);
    }

    /**
     * 计算购买路径长度
     * @param config
     * @param container
     * @return
     */
    private Optional<EvaluatedResult> compute(Config config, UserEventContainer container) {
        Optional<EvaluatedResult> result = Optional.empty();
        String channel = config.getChannel();

        int historyPurchaseTimes = config.getHistoryPurchaseTimes();  // 历史购买次数
        int maxPurchasePathLength = config.getMaxPurchasePathLength();// 购买路径

        int purchasePathLen = container.getUserEvents().size();
        //大于最大消费路径
        if (historyPurchaseTimes < 10 && purchasePathLen > maxPurchasePathLength) {
            // sort by event time
            container.getUserEvents().sort(Comparator.comparingLong(UserEvent::getEventTime));

            final Map<String, Integer> stat = Maps.newHashMap();
            container.getUserEvents()
                    .stream()
                    .collect(Collectors.groupingBy(UserEvent::getEventType))
                    .forEach((eventType, events) -> stat.put(eventType, events.size()));

            // 返回的结果
            final EvaluatedResult evaluatedResult = new EvaluatedResult();
            evaluatedResult.setUserId(container.getUserId());
            evaluatedResult.setChannel(channel);

            //
            evaluatedResult.setEventTypeCounts(stat);

            //
            evaluatedResult.setPurchasePathLength(purchasePathLen);

            log.info("Evaluated result: " + JSON.toJSONString(evaluatedResult));
            result = Optional.of(evaluatedResult);
        }
        return result;
    }
}
