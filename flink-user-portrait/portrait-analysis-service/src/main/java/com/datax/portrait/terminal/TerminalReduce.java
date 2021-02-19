package com.datax.portrait.terminal;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 *
 */
public class TerminalReduce implements ReduceFunction<TerminalInfo> {


    @Override
    public TerminalInfo reduce(TerminalInfo terminalInfo, TerminalInfo t1) throws Exception {

        String terminalType = terminalInfo.getTerminalType();

        Long count1 = terminalInfo.getCount();
        Long count2 = t1.getCount();

        TerminalInfo finalType = new TerminalInfo();
        finalType.setTerminalType(terminalType);
        finalType.setCount(count1 + count2);
        return finalType;
    }


}
