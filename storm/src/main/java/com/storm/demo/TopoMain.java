package com.storm.demo;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by xianwei on 2015/11/24.
 * 组织各个处理组件形成一个完成的处理流程，就是所谓的topology
 * 并且将改topology提交给storm集群去运行
 * topology提交到集群后永无休止的运行，出发出现错误或任务停止
 */
public class TopoMain {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        //将spout组件设置到到topology中,最后一个参数表示并发数 此处为 4个并发数
        builder.setSpout("randomspout", new RandomWordSpout(), 4);

        //将大写转换bolt组件设置到topology，并且指定他接收randomspout组件的消息
        //shuffleGrouping 随机分组
        builder.setBolt("upperbolt",new UpperBolt(),4).shuffleGrouping("randomspout");

        //添加后缀的bolt组件设置到topology，并指定他接收upperbolt组件的消息
        builder.setBolt("suffixbolt",new SuffixBolt(),4).shuffleGrouping("upperbolt");

        //用built来创建一个topology
        StormTopology topology = builder.createTopology();

        //配置一个topology在集群中运行的参数
        Config config = new Config();
        config.setNumWorkers(4);
        config.setDebug(true);
        config.setNumAckers(0);

        //提交topology
        StormSubmitter.submitTopology("demotopo",config,topology);


    }
}
