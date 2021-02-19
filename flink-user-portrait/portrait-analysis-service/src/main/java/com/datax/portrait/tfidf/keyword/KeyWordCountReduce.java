package com.datax.portrait.tfidf.keyword;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 *
 */
public class KeyWordCountReduce implements ReduceFunction<KeyWordEntity> {


    @Override
    public KeyWordEntity reduce(KeyWordEntity keyWordEntity1, KeyWordEntity keyWordEntity2) throws Exception {

        long count1 = keyWordEntity1.getTotalDocumet() == null ? 1l : keyWordEntity1.getTotalDocumet();
        long count2 = keyWordEntity2.getTotalDocumet() == null ? 1l : keyWordEntity2.getTotalDocumet();
        KeyWordEntity keyWordEntityfinal = new KeyWordEntity();
        keyWordEntityfinal.setTotalDocumet(count1 + count2);
        return keyWordEntityfinal;
    }

}
