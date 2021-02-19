package com.datax.portrait.tfidf.keyword;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;


/**
 * 一条数据 ：
 * userId , description
 * 1 , 小米8 全面屏游戏智能手机 6GB+64GB 金色 全网通4G 双卡双待
 */
public class KeywordMap implements MapFunction<String, KeyWordEntity> {

    @Override
    public KeyWordEntity map(String s) throws Exception {

        String userId = s.split(",")[0];
        String wordArray = s.split(",")[1];

        KeyWordEntity keyWordEntity = new KeyWordEntity();
        keyWordEntity.setUserId(userId);

        List<String> words = new ArrayList();
        words.add(wordArray);
        keyWordEntity.setOriginalWords(words);
        return keyWordEntity;
    }
}
