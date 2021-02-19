package com.datax.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * ik分词器的工具类
 */
public class IkUtils {

    private static Analyzer ikAnalyzer = new IKAnalyzer(true);



    public static List<String> getIkWords(String word) {
        List<String> resultList = new ArrayList();
        StringReader reader = new StringReader(word);
        //分词
        TokenStream ts = null;
        try {
            ts = ikAnalyzer.tokenStream("", reader);

            /**
             * 代码少了这行，是不行的
             */
            ts.reset();
        } catch (IOException e) {
            e.printStackTrace();
        }
        CharTermAttribute term = ts.getAttribute(CharTermAttribute.class);
        //遍历分词数据
        try {
            while (ts.incrementToken()) {
                String result = term.toString();
                resultList.add(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        reader.close();
        return resultList;
    }
}
