package com.datax.stream.dataskew;

import java.io.Serializable;

public class CountRecord  implements Serializable {
    public String key;
    public Long count;

    public CountRecord(){

    }

    public CountRecord(String key, Long count) {
        this.key = key;
        this.count = count;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}