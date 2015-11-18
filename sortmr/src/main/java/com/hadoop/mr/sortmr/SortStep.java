package com.hadoop.mr.sortmr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by xianwei on 2015/8/16.
 */
//排序步奏
public class SortStep {
    //Hadoop排序是根据k2排序的  排序的话 只要将排序Class设置为 k2
    public static class SortMapper extends Mapper<LongWritable, Text, InfoBean, NullWritable> {
        private InfoBean bean = new InfoBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            String account = fields[0];
            double in = Double.parseDouble(fields[1]);
            double out = Double.parseDouble(fields[2]);

            bean.set(account, in, out);
            context.write(bean, NullWritable.get());
        }
    }

    public static class SortReducer extends Reducer<InfoBean, NullWritable, Text, InfoBean> {
        private Text k = new Text();

        @Override
        protected void reduce(InfoBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String account = key.getAccount();
            k.set(account);
            context.write(k, key);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(SortStep.class);
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(InfoBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

//        当Reducer输入的键值和输出的键值不同时 不能使用合成器Combiner代码
//        job.setCombinerClass(SortReducer.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoBean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
