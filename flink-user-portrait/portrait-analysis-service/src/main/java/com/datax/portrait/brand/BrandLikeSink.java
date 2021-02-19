package com.datax.portrait.brand;


import com.datax.util.MongoUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

/**
 * 自定义 mongodb sink
 */
public class BrandLikeSink implements SinkFunction<BrandLike> {


    @Override
    public void invoke(BrandLike value, Context context) throws Exception {
        String brand = value.getBrand();
        long count = value.getCount();


        Document doc = MongoUtils.findOneBy("brand_like_statics", "portrait", brand);
        if (doc == null) {
            doc = new Document();
            doc.put("info", brand);
            doc.put("count", count);
        } else {
            Long countpre = doc.getLong("count");
            Long total = countpre + count;
            doc.put("count", total);
        }
        MongoUtils.saveOrUpdateMongo("brand_like_statics", "portrait", doc);
    }
}
