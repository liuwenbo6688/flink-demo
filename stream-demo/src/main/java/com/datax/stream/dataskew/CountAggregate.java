package com.datax.stream.dataskew;

import org.apache.flink.api.common.functions.AggregateFunction;



public class CountAggregate implements AggregateFunction<Record, CountRecord, CountRecord> {


    @Override
    public CountRecord createAccumulator() {
        return new CountRecord(null, 0L);
    }

    @Override
    public CountRecord add(Record value, CountRecord accumulator) {

        if(accumulator.getKey() == null){
            accumulator.setKey(value.key);
        }
        accumulator.setCount(accumulator.count + value.count);
        return accumulator;
    }

    @Override
    public CountRecord getResult(CountRecord accumulator) {
        return accumulator;
    }

    @Override
    public CountRecord merge(CountRecord a, CountRecord b) {
        System.out.println("mergemergemergemergemergemergemergemerge");
        return new CountRecord(a.getKey(), a.getCount() + b.getCount());
    }
}//