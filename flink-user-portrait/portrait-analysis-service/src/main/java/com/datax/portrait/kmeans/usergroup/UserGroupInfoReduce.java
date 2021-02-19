package com.datax.portrait.kmeans.usergroup;

import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.List;



/**
 *
 */
public class UserGroupInfoReduce implements ReduceFunction<UserGroupInfo> {


    @Override
    public UserGroupInfo reduce(UserGroupInfo userGroupInfo1, UserGroupInfo userGroupInfo2) throws Exception {


        String userId = userGroupInfo1.getUserId();

        List<UserGroupInfo> list1 = userGroupInfo1.getList();
        List<UserGroupInfo> list2 = userGroupInfo2.getList();

        UserGroupInfo userGroupInfoFinal = new UserGroupInfo();
        userGroupInfoFinal.setUserId(userId);

        List<UserGroupInfo> finalList = new ArrayList<>();
        finalList.addAll(list1);
        finalList.addAll(list2);
        userGroupInfoFinal.setList(finalList);


        return userGroupInfoFinal;
    }


}
