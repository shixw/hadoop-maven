package com.kafka.demo;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by xianwei on 2015/11/25.
 *
 */
public class ProduceDemo {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("zk.connect", "hadoop04:2181,hadoop05:2181,hadoop06:2181");
        properties.put("metadata.broker.list", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(properties);
        Producer<String, String> producer = new Producer<String, String>(config);

        //发送业务消息
        //读取文件 读取内存数据 读socket 端口
        for (int i = 0; i < 5; i++) {
            Thread.sleep(500);
            producer.send(new KeyedMessage<String, String>("workcount", "i said i love you baby for " + i));
        }
        producer.close();
    }
}
