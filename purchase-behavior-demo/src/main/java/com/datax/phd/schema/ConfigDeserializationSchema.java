package com.datax.phd.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.datax.phd.model.Config;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/1/27 下午9:08
 */
public class ConfigDeserializationSchema implements KeyedDeserializationSchema<Config> {
    @Override
    public Config deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        return JSON.parseObject(new String(message), new TypeReference<Config>() {});
    }

    @Override
    public boolean isEndOfStream(Config nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Config> getProducedType() {
        return TypeInformation.of(new TypeHint<Config>() {});
    }
}
