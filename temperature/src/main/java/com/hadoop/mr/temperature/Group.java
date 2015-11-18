package com.hadoop.mr.temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by xianwei on 2015/8/23.
 * 分组类
 */
public class Group extends WritableComparator {
    public Group() {
        super(KeyPari.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        KeyPari o1 = (KeyPari) a;
        KeyPari o2 = (KeyPari) b;
        return Integer.compare(o1.getYear(), o2.getYear());
    }
}
