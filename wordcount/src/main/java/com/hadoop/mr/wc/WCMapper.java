package com.hadoop.mr.wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by xianwei on 2015/8/14.
 * 继承Mapper  实现 map 方法
 * Hadoop 序列化实现方式与Java有所区别，它将Java中类型进行了重写，实现自己的序列化
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //接收数据  v1 (value)
        String line = value.toString();
        //切分数据
        String[] words = line.split(" ");
        //循环遍历 words
        for (String w : words) {
            //出现一次，即记一次,,然后输出
            context.write(new Text(w),new LongWritable(1L));
        }

    }
}
