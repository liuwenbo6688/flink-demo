package com.datax.portrait.wasteful;

import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class WasteReduce implements ReduceFunction<WasteInfo>{


    @Override
    public WasteInfo reduce(WasteInfo baiJiaInfo, WasteInfo t1) throws Exception {


        String userId = baiJiaInfo.getUserId();

        List<WasteInfo> wasteList1 = baiJiaInfo.getList();
        List<WasteInfo> wasteList2 = t1.getList();

        List<WasteInfo> finalList = new ArrayList<WasteInfo>();
        finalList.addAll(wasteList1);
        finalList.addAll(wasteList2);


        /**
         * 聚合一下返回
         */
        WasteInfo ret = new WasteInfo();
        ret.setUserId(userId);
        ret.setList(finalList);

        return ret;
    }
}
