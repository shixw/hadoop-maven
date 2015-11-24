package com.storm.demo;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Created by xianwei on 2015/11/24.
 *
 */
public class SuffixBolt extends BaseBasicBolt {

    FileWriter fileWriter = null;

    //在bolt组件运行过程中只会被调用一次
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            fileWriter = new FileWriter("/home/hadoop/stormoutput/" + UUID.randomUUID());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //该bolt组件的核心处理逻辑
    //没收到一个tuple消息，就会被调用一次
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //先拿到上一个组件发送过来的商品名称
        String upper_name = tuple.getString(0);
        String suffix_name = upper_name + "_itisok";

        try {
            fileWriter.write(suffix_name);
            fileWriter.write("\n");
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    //次bolt已经不需要发送tuple消息到下一个组件，所以需要再声明tuple的字段
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
