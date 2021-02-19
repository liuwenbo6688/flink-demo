package com.datax.portrait.terminal;


import com.datax.util.MongoUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

/**
 * Created by li on 2019/1/6.
 */
public class TerminalSink implements SinkFunction<TerminalInfo> {
    @Override
    public void invoke(TerminalInfo value, Context context) throws Exception {
        String terminal = value.getTerminalType();
        long count = value.getCount();

        Document doc = MongoUtils.findOneBy("use_type_statics", "portrait", terminal);
        if (doc == null) {
            doc = new Document();
            doc.put("info", terminal);
            doc.put("count", count);
        } else {
            Long countpre = doc.getLong("count");
            Long total = countpre + count;
            doc.put("count", total);
        }
        MongoUtils.saveOrUpdateMongo("use_type_statics", "portrait", doc);
    }
}
