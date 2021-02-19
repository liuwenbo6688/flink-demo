package com.datax.portrait.tfidf.keyword;

import java.util.List;
import java.util.Map;

/**
 * Created by li on 2019/1/20.
 */
public class KeyWordEntity {

    private String userId;

    // Map<单词, 出现次数>
    private Map<String,Long> dataMap;

    // Map<单词, tf=出现次数/总词频数>
    private Map<String,Double> tfMap;

    private Long totalDocumet;

    //tf/idf 计算后的前三名
    private List<String> finalKeyword;

    /**
     * 一个用户购买的所有商品描述
     */
    private List<String> originalWords;

    public List<String> getOriginalWords() {
        return originalWords;
    }

    public void setOriginalWords(List<String> originalWords) {
        this.originalWords = originalWords;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Map<String, Long> getDataMap() {
        return dataMap;
    }

    public void setDataMap(Map<String, Long> dataMap) {
        this.dataMap = dataMap;
    }

    public Map<String, Double> getTfMap() {
        return tfMap;
    }

    public void setTfMap(Map<String, Double> tfMap) {
        this.tfMap = tfMap;
    }

    public Long getTotalDocumet() {
        return totalDocumet;
    }

    public void setTotalDocumet(Long totalDocumet) {
        this.totalDocumet = totalDocumet;
    }

    public List<String> getFinalKeyword() {
        return finalKeyword;
    }

    public void setFinalKeyword(List<String> finalKeyword) {
        this.finalKeyword = finalKeyword;
    }
}
