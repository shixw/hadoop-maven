package com.storm.kafka.demo.bolt;

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
 * Created by xianwei on 2015/11/25.
 *
 */
public class WriterBolt extends BaseBasicBolt {
    private static final long serialVersionUID = -2118039784485510600L;

    private FileWriter writer = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            writer = new FileWriter("c:\\storm-kafka\\"+ UUID.randomUUID().toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String s = tuple.getString(0);
        try {
            writer.write(s);
            writer.write("\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
