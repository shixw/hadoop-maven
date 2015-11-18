package com.hadoop.mr.ii;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by xianwei on 2015/8/17.
 */
public class InverseIndex {
    public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            //返回当前Mapper对应的切片
            //InputSplit 为抽象类  可以强制转为其子类
            FileSplit split = (FileSplit) context.getInputSplit();
            //由于可能多个目录存在相同文件  需要获取文件夹目录和文件名 实例中不做演示
            String path = split.getPath().toString();
            for (String w : words) {
                k.set(w + "->" + path);
                v.set("1");
                context.write(k, v);
            }
        }
    }

    /**
     * 在Mapper和Reducer中间增加Combiner
     * 这种方式在实验环境可以使用，在集群环境下会出现问题
     * 问题？？？？
     */
    public static class IndexCombiner extends Reducer<Text, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            String[] wordAndPath = key.toString().split("->");
            String word = wordAndPath[0];
            String path = wordAndPath[1];
            for (Text t : values) {
                sum += Integer.parseInt(t.toString());
            }
            k.set(word);
            v.set(path + "->" + sum);
            context.write(k, v);
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        private Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder result = new StringBuilder();
            for (Text t : values) {
                result.append(t.toString() + "\t");
            }
            v.set(result.toString());
            context.write(key, v);

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(InverseIndex.class);
        job.setMapperClass(IndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setCombinerClass(IndexCombiner.class);
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
