package com.hadoop.mr.temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by xianwei on 2015/8/23.
 * 排序实现代码
 */
public class SortHot extends WritableComparator {
    public SortHot() {
        super(KeyPari.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        KeyPari o1 = (KeyPari) a;
        KeyPari o2 = (KeyPari) b;
        int res = Integer.compare(o1.getYear(),o2.getYear());
        if(res!=0){
            return res;
        }
        //降序
        return -Integer.compare(o1.getHot(),o2.getHot());
    }
}
