package com.datax.stream.sideoutput;


import java.util.HashMap;
import java.util.Map;

/*
{
    "name": "cpu",
    "timestamp": 1571108814142,
    "fields": {
        "usedPercent": 93.896484375,
        "max": 2048,
        "used": 1923
    },
    "tags": {
        "cluster_name": "zhisheng",
        "host_ip": "121.12.17.11"
    }
}
 */
public class MetricEvent {


	public MetricEvent() {
		tags = new HashMap<>();
		fields =  new HashMap<>();
	}

	/**
	 * Metric name
	 * 指标名
	 */
	private String name;

	/**
	 * Metric timestamp
	 * 数据时间
	 */
	private Long timestamp;

	/**
	 * Metric fields
	 * 指标具体字段
	 */
	private Map<String, Object> fields;

	/**
	 * Metric tags
	 * 指标的标识
	 */
	private Map<String, String> tags;


	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Map<String, Object> getFields() {
		return fields;
	}

	public void setFields(Map<String, Object> fields) {
		this.fields = fields;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}


	@Override
	public String toString() {
		return "MetricEvent{" +
				"name='" + name + '\'' +
				", timestamp=" + timestamp +
				", fields=" + fields +
				", tags=" + tags +
				'}';
	}
}