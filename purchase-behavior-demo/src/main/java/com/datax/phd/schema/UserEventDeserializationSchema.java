package com.datax.phd.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.datax.phd.model.UserEvent;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

/**
 *
 */
public class UserEventDeserializationSchema implements KeyedDeserializationSchema<UserEvent> {
    @Override
    public UserEvent deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {

        // 把json字符串 转换为  UserEvent对象
        return JSON.parseObject(new String(message), new TypeReference<UserEvent>() {});
    }

    // kafka  固定就完了
    @Override
    public boolean isEndOfStream(UserEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<UserEvent> getProducedType() {
        return TypeInformation.of(new TypeHint<UserEvent>() {});
    }
}
