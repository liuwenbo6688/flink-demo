package com.datax.portrait.tfidf;


import com.datax.util.IkUtils;
import com.datax.util.MapUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;


/**
 * java 实现 TF/IDF
 *
 * TFw = 在某一类中词条w出现的次数 / 该类中所有的词条数目
 *
 * IDF = log(语料库的文档总数 / (包含词条w的文档数+1)),分母之所以要加1，是为了避免分母为0

 *
 */
public class TFIDFMain {



    public static void main(String[] args) throws Exception {




        //  <文档, Map<单词, 出现次数>>
        Map<String, Map<String, Long>> documentTfMap = new HashMap();

        String filepath = "E:\\github_workspace\\flink-user-portrait\\portrait-analysis-service\\src\\main\\resources\\文章.txt";
        File file = new File(filepath);

        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String temp = "";

        // 所有单词的去重
        Set<String> wordSet = new HashSet();

        // 暂存所有文档
        List<String> dataList = new ArrayList();


        /**
         *  以文档为维度，统计每个单词出现的次数
         */
        while ((temp = bufferedReader.readLine()) != null) {
            List<String> words = IkUtils.getIkWords(temp);
            String docId = UUID.randomUUID().toString();

            dataList.add(temp);

            for (String inner : words) {
                /**
                 * 以文档为维度，统计每个单词出现的次数
                 */
                Map<String, Long> tfmap = documentTfMap.get(docId) == null ? new HashMap() : documentTfMap.get(docId);
                Long pre = tfmap.get(inner) == null ? 0l : tfmap.get(inner);
                tfmap.put(inner, pre + 1);
                documentTfMap.put(docId, tfmap);

                wordSet.add(inner);
            }
        }


        /**
         * 统计idf的次数，
         * 统计单词，在多少个文档中出现过
         */
        Map<String, Long> idfMap = new HashMap();
        for (String word : wordSet) {
            for (String tempdata : dataList) {
                if (IkUtils.getIkWords(tempdata).contains(word)) {
                    Long pre = idfMap.get(word) == null ? 0l : idfMap.get(word);
                    idfMap.put(word, pre + 1);
                }
            }
        }


        int allDocumentNums = documentTfMap.keySet().size();


        for (Map.Entry<String, Map<String, Long>> entry : documentTfMap.entrySet()) {
            String documentId = entry.getKey();
            Map<String, Double> tfidfMap = new HashMap();

            Map<String, Long> tfMapTemp = entry.getValue();

            Collection<Long> collections = tfMapTemp.values();
            long sumTf = 0l;// 一个文档中所有词条的总数
            for (Long templong : collections) {
                sumTf += templong;
            }


            for (Map.Entry<String, Long> entrytf : tfMapTemp.entrySet()) {
                String word = entrytf.getKey();
                long count = entrytf.getValue();

                // TF = 在某一类中词条w出现的次数 / 该类中所有的词条数目
                double tf = Double.valueOf(count) / Double.valueOf(sumTf);

                // IDF = log(语料库的文档总数 / (包含词条w的文档数+1))
                double idf = Math.log(Double.valueOf(allDocumentNums) / Double.valueOf(idfMap.get(word) + 1));

                double tfIdf = tf * idf;
                tfidfMap.put(word, tfIdf);
            }

            LinkedHashMap<String, Double> sortedMap = MapUtils.sortMapByValue(tfidfMap);

            System.out.println("============================================================>" + documentId);
            int count = 1;
            for (Map.Entry<String, Double> entryfinal : sortedMap.entrySet()) {
                if (count > 3) {
                    break;
                }
                System.out.println(entryfinal.getKey());
                count++;
            }



        }

    }
}
