package com.datax.phd.schema;
import com.alibaba.fastjson.JSON;
import com.datax.phd.model.EvaluatedResult;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/1/27 下午10:40
 */
public class EvaluatedResultSerializationSchema implements KeyedSerializationSchema<EvaluatedResult> {
    @Override
    public byte[] serializeKey(EvaluatedResult element) {
        return element.getUserId().getBytes();
    }

    @Override
    public byte[] serializeValue(EvaluatedResult element) {
        return JSON.toJSONString(element).getBytes();
    }

    @Override
    public String getTargetTopic(EvaluatedResult element) {
        return null;
    }
}
