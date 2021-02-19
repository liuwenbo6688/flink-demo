package com.datax.portrait.tfidf.keyword;

import com.datax.util.HbaseUtils;
import com.datax.util.MapUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

/**
 *
 */

public class KeywordTfIdfMap2 implements MapFunction<KeyWordEntity, KeyWordEntity> {

    private long totaldoucments = 0l;
    private long words;
    private String columnName;

    public KeywordTfIdfMap2(long totaldoucments, long words, String columnName) {
        this.totaldoucments = totaldoucments;
        this.words = words;
        this.columnName = columnName;

    }

    @Override
    public KeyWordEntity map(KeyWordEntity keyWordEntity) throws Exception {

        Map<String, Double> tfidfmap = new HashMap();

        String userid = keyWordEntity.getUserId();

        Map<String, Double> tfmap = keyWordEntity.getTfMap();


        String tablename = "keyword_data";
        String famliyname = "base_info";
        String colum = "idf_count";

        for (Map.Entry<String, Double> entry : tfmap.entrySet()) {
            String word = entry.getKey();
            Double value = entry.getValue();
            /**
             * hbase查询，分词出现在多少文档中
             */
            String data = HbaseUtils.getdata(tablename, word, famliyname, colum);
            long viewcount = Long.valueOf(data);

            // IDF = log(语料库的文档总数 / (包含词条w的文档数+1))
            Double idf = Math.log(totaldoucments / (viewcount + 1));
            Double tfidf = value * idf;
            tfidfmap.put(word, tfidf);
        }

        LinkedHashMap<String, Double> resultFinal = MapUtils.sortMapByValue(tfidfmap);

        List<String> finalword = new ArrayList();// 保留前三名
        int count = 1;
        for (Map.Entry<String, Double> mapentry : resultFinal.entrySet()) {
            finalword.add(mapentry.getKey());
            count++;
            if (count > words) {
                break;
            }
        }

        KeyWordEntity keyWordEntityfinal = new KeyWordEntity();
        keyWordEntityfinal.setUserId(userid);
        keyWordEntityfinal.setFinalKeyword(finalword);

        String keywordstring = "";
        for (String keyword : finalword) {
            keywordstring += keyword + ",";
        }
        if (StringUtils.isNotBlank(keywordstring)) {
            String tablename1 = "user_keyword_label";
            String rowkey1 = userid;
            String famliyname1 = "base_info";
            String colum1 = columnName;
            HbaseUtils.putdata(tablename1, rowkey1, famliyname1, colum1, keywordstring);
        }


        return keyWordEntityfinal;
    }
}
