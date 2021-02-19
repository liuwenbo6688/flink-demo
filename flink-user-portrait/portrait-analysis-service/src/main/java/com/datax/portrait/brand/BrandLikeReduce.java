package com.datax.portrait.brand;


import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Created by li on 2019/1/6.
 */
public class BrandLikeReduce implements ReduceFunction<BrandLike> {


    @Override
    public BrandLike reduce(BrandLike brandLike, BrandLike t1) throws Exception {


        String brand = brandLike.getBrand();
        long count1 = brandLike.getCount();
        long count2 = t1.getCount();

        BrandLike brandLikeFinal = new BrandLike();
        brandLikeFinal.setBrand(brand);
        brandLikeFinal.setCount(count1 + count2);

        return brandLikeFinal;
    }


}
