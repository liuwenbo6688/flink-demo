package com.datax.portrait.logistic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * java 实现的单机版逻辑回归
 *
 * 随机梯度下降的方式
 *
 *
 */
public class Logistic {

    public static void main(String[] args) {
        colicTest();
    }


    /**
     * @param inX
     * @param weights
     * @return
     */
    public static String classifyVector(ArrayList<String> inX, ArrayList<Double> weights) {
        ArrayList<Double> sum = new ArrayList<Double>();
        sum.clear();
        sum.add(0.0);
        for (int i = 0; i < inX.size(); i++) {
            sum.set(0, sum.get(0) + Double.parseDouble(inX.get(i)) * weights.get(i));
        }
        /**
         * 大于0.5 就是 1
         * 小于0.5 就是 0
         */
        if (sigmoid(sum).get(0) > 0.5)
            return "1";
        else
            return "0";

    }

    /**
     * 使用的是随机梯度下降求解的方式，
     */
    public static void colicTest() {


        // 鸢尾花训练数据集
        String trainingFile = "E:\\github_workspace\\flink-user-portrait\\portrait-analysis-service\\src\\main\\resources\\Training.txt";
        // 鸢尾花测试数据集
        String testFile = "E:\\github_workspace\\flink-user-portrait\\portrait-analysis-service\\src\\main\\resources\\Test.txt";


        CreateDataSet trainingSet = readFile(trainingFile);
        CreateDataSet testSet = readFile(testFile);


        /**
         *  梯度下降进行逻辑回归的求解
         */
        ArrayList<Double> weights = gradAscent(trainingSet, trainingSet.labels, 500);

        System.out.println(weights);


        /**
         * 预测
         */
        int errorCount = 0;
        for (int i = 0; i < testSet.data.size(); i++) {
            if (!classifyVector(testSet.data.get(i), weights).equals(testSet.labels.get(i))) {
                errorCount++;
            }
//            System.out.println(classifyVector(testSet.data.get(i), weights) + "," + testSet.labels.get(i));
        }

        System.out.println("预测失败率：" + 1.0 * errorCount / testSet.data.size());

    }

    /**
     * @param inX
     * @return
     * @Description: [sigmod函数]  1 / (1 + e ^ -t)
     */
    public static ArrayList<Double> sigmoid(ArrayList<Double> inX) {
        ArrayList<Double> inXExp = new ArrayList<Double>();
        for (int i = 0; i < inX.size(); i++) {
            inXExp.add(1.0 / (1 + Math.exp(-inX.get(i))));
        }
        return inXExp;
    }

    /**
     * @param dataSet
     * @param classLabels
     * @param numberIter
     * @return
     */
    public static ArrayList<Double> gradAscent(Matrix dataSet, ArrayList<String> classLabels, int numberIter) {


        // 测试样本的数量
        int m = dataSet.data.size();

        // 特征数
        int n = dataSet.data.get(0).size();

        // 学习率
        double alpha = 0.0;

        // 随机梯度下降，随机选取的样本行号
        int randIndex = 0;

        //  [θ(0),θ(1),..., θ(n-1)]
        //  初始化全是1，就是最终求解的 theta数组
        ArrayList<Double> weights = new ArrayList<Double>();


//        ArrayList<Double> weightstmp = new ArrayList<Double>();
//        ArrayList<Double> h = new ArrayList<Double>();

        // 保存样本的行号
        ArrayList<Integer> dataIndex = new ArrayList<Integer>();


        ArrayList<Double> dataMatrixMulweights = new ArrayList<Double>();

        for (int i = 0; i < n; i++) {
            weights.add(1.0); //  theta数组初始化为1
//            weightstmp.add(1.0);
        }
        dataMatrixMulweights.add(0.0);
        double error = 0.0;


        for (int j = 0; j < numberIter; j++) { //numberIter 迭代几圈，每一圈都要把所有样本都测试一遍

            // 产生0到99的数组
            for (int p = 0; p < m; p++) {
                // 0 到 (m-1)，像是加一个行号，随机梯度下降选择某一行数据使用
                dataIndex.add(p);
            }


            for (int i = 0; i < m; i++) {

                // 步长，随着迭代，逐渐变小
                alpha = 4 / (1.0 + i + j) + 0.0001;

                /**
                 * 随机梯度下降，随机选一条数据，然后从dataIndex删除，下次不会选择这条数据
                 * 就是把每个样本都迭代一遍
                 */
                randIndex = (int) (Math.random() * dataIndex.size());
                dataIndex.remove(randIndex);


                // 求和：h(x) =  θ(0)*x(0) + θ(1)*x(1) +... +  θ(n-1)*x(n-1)
                double temp = 0.0;
                for (int k = 0; k < n; k++) {
                    temp = temp +
                            Double.parseDouble(dataSet.data.get(randIndex).get(k)) * weights.get(k);
                }

                dataMatrixMulweights.set(0, temp);

                // 结果放进去，求 sigmoid
                // 相当于：sigmoid(X_b.dot(theta))
                ArrayList<Double> h = sigmoid(dataMatrixMulweights);

                // 差值和真实值的差额  h(x) - y
                // 相当于：sigmoid(X_b.dot(theta)) - y
                error = Double.parseDouble(classLabels.get(randIndex)) - h.get(0);

                double tempWeight = 0.0;
                for (int p = 0; p < n; p++) {
                    // (h(x) - y) * x(i)
                    // 相当于：eta * dJ = eta * X_b.T.dot( sigmoid(X_b.dot(theta)) - y )
                    tempWeight = alpha
                            * Double.parseDouble(dataSet.data.get(randIndex).get(p))  //  x(i)
                            * error; // (h(x) - y)
                    /**
                     * 因为是随机梯度下降，不用除以m
                     */

                    // 梯度的方向,移动
                    weights.set(p, weights.get(p) + tempWeight);
                }


            }

        }


        return weights;
    }


    public Logistic() {
        super();
    }

    /**
     * @param fileName 读入的文件名
     * @return
     */
    public static CreateDataSet readFile(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        CreateDataSet dataSet = new CreateDataSet();
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                String[] strArr = tempString.split(",");
                ArrayList<String> as = new ArrayList<String>();
                as.add("1"); //x0 值默认是1 ，  θ(0)*x(0) ，截距
                for (int i = 0; i < strArr.length - 1; i++) {
                    as.add(strArr[i]);
                }
                dataSet.data.add(as);

                // 最后一列是标签属性
                dataSet.labels.add(strArr[strArr.length - 1]);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return dataSet;
    }

}