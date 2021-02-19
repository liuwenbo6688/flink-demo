package com.datax.portrait.tfidf.keyword;

import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 按用户id分组，每个用户组成一个文档
 */
public class KeywordReduce implements ReduceFunction<KeyWordEntity>{


    @Override
    public KeyWordEntity reduce(KeyWordEntity keyWordEntity1, KeyWordEntity keyWordEntity2) throws Exception {

        String userId = keyWordEntity1.getUserId();

        List<String> words1 = keyWordEntity1.getOriginalWords();
        List<String> words2 = keyWordEntity2.getOriginalWords();

        List<String> finalwords = new ArrayList<String>();
        finalwords.addAll(words1);
        finalwords.addAll(words2);

        KeyWordEntity keyWordEntityfinal = new KeyWordEntity();
        keyWordEntityfinal.setOriginalWords(finalwords);
        keyWordEntityfinal.setUserId(userId);
        return keyWordEntityfinal;
    }
}
