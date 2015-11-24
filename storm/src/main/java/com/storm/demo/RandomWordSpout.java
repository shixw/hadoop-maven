package com.storm.demo;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by xianwei on 2015/11/24.
 * Storm Spout demo
 */
public class RandomWordSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    //模拟一下数据
    String[] words = {"iphone", "xiaomi", "mate", "sony", "sumsung", "moto", "meizu"};

    //不断地往下一个组件发送tuple消息
    //    这里面是该spout组件的核心逻辑
    @Override
    public void nextTuple() {
        //可以从kafka消息队列中拿到数据，简便起见，从数组works中随机挑选一个商品名发送出去
        Random random = new Random();
        int index = random.nextInt(words.length);
        //通过随机数拿到一个商品名称
        String godName = words[index];

        //将商品名称封装成tuple,发送消息给下一个组件
        collector.emit(new Values(godName));

        //没法送一个消息，休眠500ms
        Utils.sleep(500);
    }

    //声明本sqout组件发送出去的tuple中的数据的字段名
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("orignname"));
    }

    //初始化是调用open方法
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }


}
