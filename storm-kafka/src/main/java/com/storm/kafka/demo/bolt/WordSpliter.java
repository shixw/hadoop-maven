package com.storm.kafka.demo.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

/**
 * Created by xianwei on 2015/11/25.
 *
 */
public class WordSpliter extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String line = tuple.getString(0);
        String[] words = line.split(" ");
        for (String word:words){
            word = word.trim();
            if(StringUtils.isNotBlank(word)){
                word = word.toLowerCase();
                basicOutputCollector.emit(new Values(word));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
    }
}
