package com.datax.portrait.terminal;

import com.alibaba.fastjson.JSONObject;

import com.datax.portrait.kafka.KafkaEvent;
import com.datax.portrait.log.ScanProductLog;
import com.datax.util.HbaseUtils;
import com.datax.util.MapUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by li on 2019/1/6.
 */
public class TerminalFlatMap implements FlatMapFunction<KafkaEvent, TerminalInfo> {

    @Override
    public void flatMap(KafkaEvent kafkaEvent, Collector<TerminalInfo> collector) throws Exception {


        String data = kafkaEvent.getWord();
        ScanProductLog scanProductLog = JSONObject.parseObject(data, ScanProductLog.class);


        int userId = scanProductLog.getUserId();
        int terminalType = scanProductLog.getTerminalType(); //终端类型：0、pc端；1、移动端；2、小程序端


        String terminalName = terminalType == 0 ? "pc端" : terminalType == 1 ? "移动端" : "小程序端";

        String tablename = "user_flag_info";
        String rowkey = userId + "";
        String family = "user_behavior";
        String column = "use_type_list"; //运营
        String mapData = HbaseUtils.getdata(tablename, rowkey, family, column);


        Map<String, Long> map = new HashMap<>();
        if (StringUtils.isNotBlank(mapData)) {
            map = JSONObject.parseObject(mapData, Map.class);
        }

        // 获取之前的终端偏好
        String preMaxTerminal = MapUtils.getMaxByMap(map);



        long preTerminalCount = map.get(terminalName) == null ? 0l : map.get(terminalName);
        map.put(terminalName, preTerminalCount + 1);
        HbaseUtils.putdata(tablename, rowkey, family, column, JSONObject.toJSONString(map));

        String maxTerminal = MapUtils.getMaxByMap(map);

        if (StringUtils.isNotBlank(maxTerminal) && !preMaxTerminal.equals(maxTerminal)) {
            TerminalInfo terminalInfo = new TerminalInfo();
            terminalInfo.setTerminalType(preMaxTerminal);
            terminalInfo.setCount(-1l);
            terminalInfo.setGroupField("==Terminalinfo==" + preMaxTerminal);
            collector.collect(terminalInfo);
        }

        TerminalInfo terminalInfo = new TerminalInfo();
        terminalInfo.setTerminalType(maxTerminal);
        terminalInfo.setCount(1l);
        terminalInfo.setGroupField("==Terminalinfo==" + maxTerminal);
        collector.collect(terminalInfo);

        column = "use_type";
        HbaseUtils.putdata(tablename, rowkey, family, column, maxTerminal);

    }

}
