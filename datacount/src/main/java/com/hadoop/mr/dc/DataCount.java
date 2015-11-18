package com.hadoop.mr.dc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Created by xianwei on 2015/8/14.
 */
public class DataCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(DataCount.class);

        job.setMapperClass(DCMapper.class);
        //满足  k2,k2 和  k3,v3 类型一一对应时  可以省略
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setCombinerClass(DCReducer.class);
        job.setReducerClass(DCReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataBean.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //设置分区类
        job.setPartitionerClass(ProviderPartitioner.class);
        //设置分区时  需要多个Reducer 返回多个分区
        //需要设置启动多个Reducer
        //执行后会将结果返回到多个结果文件中
        //如果Reducer数量多于分区数量  则多于 的结果文件为空
        job.setNumReduceTasks(Integer.parseInt(args[2]));

        job.waitForCompletion(true);

    }

    //将结果分区
    //Partitioner 在Mapper完成，Reducer 没有开始之前执行
    public static class ProviderPartitioner extends Partitioner<Text, DataBean> {

        private static Map<String, Integer> providerMap = new HashMap<String, Integer>();

        static {
            providerMap.put("135", 1);
            providerMap.put("136", 1);
            providerMap.put("137", 1);
            providerMap.put("138", 1);
            providerMap.put("139", 1);
            providerMap.put("150", 2);
            providerMap.put("159", 2);
            providerMap.put("182", 3);
            providerMap.put("183", 3);

        }

        @Override
        public int getPartition(Text key, DataBean value, int numPartitions) {
            String accout = key.toString();
            String sub_acc = accout.substring(0, 3);
            Integer code = providerMap.get(sub_acc);
            if (code==null){
                return 0;
            }
            return code;
        }
    }

    public static class DCMapper extends Mapper<LongWritable, Text, Text, DataBean> {
        //获取数据
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            String telNo = fields[1];
            long upPayLoad = Long.parseLong(fields[8]);
            long downPayLoad = Long.parseLong(fields[9]);

            context.write(new Text(telNo), new DataBean(telNo, upPayLoad, downPayLoad));
        }
    }

    public static class DCReducer extends Reducer<Text, DataBean, Text, DataBean> {
        @Override
        protected void reduce(Text key, Iterable<DataBean> values, Context context) throws IOException, InterruptedException {
            long up_sum = 0;
            long down_sum = 0;
            for (DataBean bean : values) {
                up_sum += bean.getUpPayLoad();
                down_sum += bean.getDownPayLoad();
            }
            DataBean db = new DataBean("", up_sum, down_sum);
            context.write(key, db);

        }
    }
}

