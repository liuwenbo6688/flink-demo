package com.datax.portrait.email;


import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Created by li on 2019/1/5.
 */
public class EmailReduce implements ReduceFunction<EmaiInfo> {


    @Override
    public EmaiInfo reduce(EmaiInfo emaiInfo, EmaiInfo t1) throws Exception {

        String emailType = emaiInfo.getEmailType();

        Long count1 = emaiInfo.getCount();
        Long count2 = t1.getCount();

        EmaiInfo emaiInfoFinal = new EmaiInfo();
        emaiInfoFinal.setEmailType(emailType);
        emaiInfoFinal.setCount(count1 + count2);

        return emaiInfoFinal;
    }
}
