package com.hadoop.rpc;

/**
 * Created by xianwei on 2015/8/11.
 */
interface Bizable {
    //    hadoop 2.0 之后需要在接口中定义 版本号
    public static final long versionID = 100L;

    String sayHi(String name);
}
