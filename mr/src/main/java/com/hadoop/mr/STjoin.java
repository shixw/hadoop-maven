package com.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by xianwei on 2015/9/11.
 */
public class STjoin {
    public static int time = 0;

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String childname = new String();
            String parentname = new String();
            String relationtype = new String();

            StringTokenizer itr = new StringTokenizer(value.toString());
            String[] values = new String[2];
            int i = 0;
            while (itr.hasMoreElements()) {
                values[i] = itr.nextToken();
                i++;
            }

            if (values[0].compareTo("child") != 0) {
                childname = values[0];
                parentname = values[1];

                relationtype = "1";
                context.write(new Text(values[1]), new Text(relationtype + "+" + childname + "+" + parentname));
                relationtype = "2";
                context.write(new Text(values[0]), new Text(relationtype + "+" + childname + "+" + parentname));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (0 == time) {
                context.write(new Text("grandchild"), new Text("grandparent"));
                time++;
            }
            int grandchildnum = 0;
            String[] grandchild = new String[2];
            int grandparentnum = 0;
            String[] grandparent = new String[2];

            Iterator ite = values.iterator();
            while (ite.hasNext()) {
                String record = ite.next().toString();
                int len = record.length();
                int i = 2;
                if (0 == len) {
                    continue;
                }

                char relationtype = record.charAt(0);

                String childname = new String();
                String parentname = new String();

                while (record.charAt(i) != '+') {
                    childname += record.charAt(i);
                    i++;
                }

                i = i + 1;

                while (i < len) {
                    parentname += record.charAt(i);
                    i++;
                }

                if ('1' == relationtype) {
                    grandchild[grandchildnum] = childname;
                    grandchildnum++;
                }

                if ('2' == relationtype) {
                    grandparent[grandparentnum] = parentname;
                    grandparentnum++;
                }
            }
            if (0 != grandchildnum && 0 != grandparentnum) {
                for (int j = 0; j < grandchildnum; j++) {
                    for (int n = 0; n < grandparentnum; n++) {
                        context.write(new Text(grandchild[j]), new Text(grandparent[n]));
                    }
                }
            }

        }
    }

    public static void outStrArr(String[] s) {
        String str = "";
        for (String s1 : s) {
            str += s1+"\t";
        }
        System.out.println(str);
    }

    public static void main(String[] args) throws Exception {

        // 构建 一个 任务
        Job job = Job.getInstance(new Configuration());
        // 将main方法所在的类传入 job中
        job.setJarByClass(STjoin.class);

        // 设置Mapper
        job.setMapperClass(Map.class);
        // 设置Mapper输输出数据的类 运行时出现错误
        // job.setMapOutputKeyClass(Text.class);
        // job.setMapOutputValueClass(LongWritable.class);

        // 设置输入数据的路径
        FileInputFormat.setInputPaths(job, new Path("hdfs://hadooptest:9000/STjoin"));

        // 优化Reducer执行速度 增加后速度大幅提升
        // 在Reducer之前执行 首先合并一下 是一个特殊的Reducer
        // 设置Reducer
        job.setReducerClass(Reduce.class);

        // 设置输出 没有指定具体为 Mapper 还是 Reducer 的输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置计算后的输出的路径
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadooptest:9000/STjoin15"));

        // 提交 并 等待响应 true 表示打印执行进度
        job.waitForCompletion(true);
    }
}
