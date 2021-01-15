package com.datax.stream.processfunction.alert;


import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;


public class OutageMetricWaterMark implements AssignerWithPeriodicWatermarks<OutageMetricEvent> {
    private long currentTimestamp = Long.MIN_VALUE;

    private final long maxTimeLag = 5000;

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
    }

    @Override
    public long extractTimestamp(OutageMetricEvent outageMetricEvent, long l) {
        long timestamp = outageMetricEvent.getTimestamp();
        currentTimestamp = Math.max(timestamp, currentTimestamp);
        return timestamp;
    }
}