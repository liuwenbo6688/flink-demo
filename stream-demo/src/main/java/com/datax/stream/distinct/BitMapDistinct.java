package com.datax.stream.distinct;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

/**
 * 海量数据去重方案
 */
public class BitMapDistinct implements AggregateFunction<Long, Roaring64NavigableMap,Long> {


    @Override
    public Roaring64NavigableMap createAccumulator() {
        return new Roaring64NavigableMap();
    }

    @Override
    public Roaring64NavigableMap add(Long value, Roaring64NavigableMap accumulator) {
        accumulator.add(value);
        return accumulator;
    }


    @Override
    public Long getResult(Roaring64NavigableMap accumulator) {
        // 获取结果
        return accumulator.getLongCardinality();
    }

    @Override
    public Roaring64NavigableMap merge(Roaring64NavigableMap a, Roaring64NavigableMap b) {
        return null;
    }
}
