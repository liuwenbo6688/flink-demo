package com.datax.portrait.yearbase;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 *
 */
public class YearBaseReduce implements ReduceFunction<YearBase>{
    @Override
    public YearBase reduce(YearBase yearBase, YearBase t1) throws Exception {


        String yearType = yearBase.getYearType();

        Long count1 = yearBase.getCount();
        Long count2 = t1.getCount();

        // new一个新的？有必要吗？
        YearBase finalYearBase = new YearBase();
        finalYearBase.setYearType(yearType);
        finalYearBase.setCount(count1 + count2); // 累积总数

        return finalYearBase;

    }
}
