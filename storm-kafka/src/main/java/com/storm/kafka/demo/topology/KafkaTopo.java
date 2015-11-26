package com.storm.kafka.demo.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.storm.kafka.demo.bolt.WordSpliter;
import com.storm.kafka.demo.bolt.WriterBolt;
import com.storm.kafka.demo.spout.MessageSpout;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by xianwei on 2015/11/25.
 *
 */
public class KafkaTopo {
    public static void main(String[] args) throws Exception{
        String topic = "wordcount";
        String zkRoot = "/kafka-storm";
        String spoutId = "KafkaSpout";

        //整合kafka  设置brober 及 spout 配置信息
        BrokerHosts brokerHosts = new ZkHosts("hadoop04:2181,hadoop05:2181,hadoop06:2181");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,topic,zkRoot,spoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageSpout());

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(spoutId, new KafkaSpout(spoutConfig));
        builder.setBolt("word-spilter",new WordSpliter(),4).shuffleGrouping(spoutId);
        builder.setBolt("writer",new WriterBolt(),4).fieldsGrouping("word-spilter", new Fields("word"));

        Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setNumAckers(0);
        conf.setDebug(true);

        //LocalCluster用来将topology提交到本地模拟器运行，方便开发调试
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("WordCount",conf,builder.createTopology());

        StormSubmitter.submitTopology("WordCount", conf, builder.createTopology());
    }
}
