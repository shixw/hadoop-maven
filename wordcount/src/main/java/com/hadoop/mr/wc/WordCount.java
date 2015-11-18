package com.hadoop.mr.wc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by xianwei on 2015/8/14.
 */
public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //构建 一个 任务
        Job job = Job.getInstance(new Configuration());
        //将main方法所在的类传入 job中
        job.setJarByClass(WordCount.class);


        //设置Mapper
        job.setMapperClass(WCMapper.class);
        //设置Mapper输输出数据的类  运行时出现错误
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(LongWritable.class);

        //设置输入数据的路径
        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01:9000/words"));

        //优化Reducer执行速度 增加后速度大幅提升
        //在Reducer之前执行 首先合并一下 是一个特殊的Reducer
        //可以用于数据过滤等
        job.setCombinerClass(WCReducer.class);
        //设置Reducer
        job.setReducerClass(WCReducer.class);

        //设置输出 没有指定具体为  Mapper 还是 Reducer 的输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置计算后的输出的路径
        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop01:9000/wccount20150814"));

        //提交 并 等待响应  true  表示打印执行进度
        job.waitForCompletion(true);

    }
}
