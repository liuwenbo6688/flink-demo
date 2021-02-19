package com.datax.portrait.brand;

import com.alibaba.fastjson.JSONObject;
import com.datax.portrait.kafka.KafkaEvent;
import com.datax.portrait.log.ScanProductLog;
import com.datax.util.HbaseUtils;
import com.datax.util.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class BrandLikeFlatMap implements FlatMapFunction<KafkaEvent, BrandLike> {


    @Override
    public void flatMap(KafkaEvent kafkaEvent, Collector<BrandLike> collector) throws Exception {


        String data = kafkaEvent.getWord();
        ScanProductLog scanProductLog = JSONObject.parseObject(data, ScanProductLog.class);


        int userId = scanProductLog.getUserId();

        String brand = scanProductLog.getBrand();

        String tablename = "user_flag_info";
        String rowkey = userId + "";
        String family = "user_behavior";
        String column = "brand_list";//运营
        String mapdata = HbaseUtils.getdata(tablename, rowkey, family, column);


        Map<String, Long> map = new HashMap<>();
        if (StringUtils.isNotBlank(mapdata)) {
            map = JSONObject.parseObject(mapdata, Map.class);
        }

        // 获取之前的品牌偏好
        String preMaxBrand = MapUtils.getMaxByMap(map);


        /**
         * 保留所有的品牌和浏览次数
         * {"小米" : 10, "华为" : 20}
         */
        long preBrand = map.get(brand) == null ? 0l : map.get(brand);
        map.put(brand, preBrand + 1);
        HbaseUtils.putdata(tablename, rowkey, family, column, JSONObject.toJSONString(map));


        /**
         * 如果前后发生品牌切换，发送一个之前品牌的 -1 消息
         */
        String maxBrand = MapUtils.getMaxByMap(map);

        if (StringUtils.isNotBlank(maxBrand)
                && preMaxBrand != null && !preMaxBrand.equals(maxBrand)) {
            BrandLike brandLike = new BrandLike();
            brandLike.setBrand(preMaxBrand);
            brandLike.setCount(-1l); // -1
            brandLike.setGroupField("==brandlike==" + preMaxBrand);
            collector.collect(brandLike);

            //TODO  是不是下面的+1消息，也要在这里面发送
        }


        /**
         * 品牌偏好 + 1
         */
        BrandLike brandLike = new BrandLike();
        brandLike.setBrand(maxBrand);
        brandLike.setCount(1l);
        brandLike.setGroupField("==brandlike==" + maxBrand);
        collector.collect(brandLike);


        column = "brandlike";
        HbaseUtils.putdata(tablename, rowkey, family, column, maxBrand);


    }

}
