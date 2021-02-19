package com.datax.portrait.tfidf.keyword;

import com.datax.util.HbaseUtils;
import com.datax.util.IkUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;


/**
 *
 */
public class KeywordTfIdfMap1 implements MapFunction<KeyWordEntity, KeyWordEntity> {

    @Override
    public KeyWordEntity map(KeyWordEntity keyWordEntity) throws Exception {

        List<String> words = keyWordEntity.getOriginalWords();

        // Map<单词, 出现次数>
        Map<String, Long> tfmap = new HashMap();
        Set<String> wordSet = new HashSet();
        for (String outerword : words) {
            List<String> listdata = IkUtils.getIkWords(outerword);
            for (String word : listdata) {
                Long pre = tfmap.get(word) == null ? 0l : tfmap.get(word);
                tfmap.put(word, pre + 1);
                wordSet.add(word);
            }
        }

        KeyWordEntity keyWordEntityfinal = new KeyWordEntity();
        String userid = keyWordEntity.getUserId();
        keyWordEntityfinal.setUserId(userid);

        /**
         *  DataMap
         */
        keyWordEntityfinal.setDataMap(tfmap);

        // 计算总数？ 这个总次数？  一个文档中所有词条的总数
        long sum = 0l;
        Collection<Long> longset = tfmap.values();
        for (Long templong : longset) {
            sum += templong;
        }

        Map<String, Double> tfmapfinal = new HashMap();
        for (Map.Entry<String, Long> entry : tfmap.entrySet()) {
            String word = entry.getKey();
            long count = entry.getValue();
            // TFw = 在某一文档中词条w出现的次数 / 该文档中中所有的词条数目
            double tf = Double.valueOf(count) / Double.valueOf(sum);
            tfmapfinal.put(word, tf);
        }

        /**
         *  TfMap
         */
        keyWordEntityfinal.setTfMap(tfmapfinal);


        /**
         * 每个单词，出现在多少文档中，使用hbase做存储
         * 对一个用户算一类
         */
        //create "keyword_data, "base_info"
        for (String word : wordSet) {
            String tablename = "keyword_data";
            String rowkey = word;
            String famliyname = "base_info";
            String colum = "idf_count";
            String data = HbaseUtils.getdata(tablename, rowkey, famliyname, colum);
            Long pre = data == null ? 0l : Long.valueOf(data);
            Long total = pre + 1; // 累加1, 单词出现的文档数
            HbaseUtils.putdata(tablename, rowkey, famliyname, colum, total + "");
        }


        return keyWordEntity;
    }
}
