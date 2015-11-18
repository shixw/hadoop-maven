package com.hadoop.mr.wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xianwei on 2015/8/14.
 */
//reduce 接收到后 数据为 {key,[1,1,1,1,1]}
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //接收数据
        //定义一个计数器
        long count = 0;
        //循环values
        for (LongWritable v : values) {
            count += v.get();
        }
        //输出
        context.write(key,new LongWritable(count));
    }
}
