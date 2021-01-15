package com.datax.stream.dataskew;


import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 *
 */

public class RecordSource extends RichSourceFunction<String> {
    private static final long serialVersionUID = 3519222623348229907L;
    private volatile boolean isRunning = true;

    private Record record = new Record();


    private static List<String> keys =  new ArrayList<>();
    static {
        keys.add("aaa");
        keys.add("bbb");
        keys.add("ccc");
        keys.add("ddd");
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        //定时读取数据库的flow表，生成FLow数据

        while (isRunning) {

            for (int i = 0; i < 100; i++) {

                record.key = keys.get(new Random().nextInt(4));
                record.count = 10l;
                ctx.collect(JSON.toJSONString(record));

            }

            System.out.println("produce 100 record " + JSON.toJSONString(record));
            Thread.sleep(20 * 1000L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
