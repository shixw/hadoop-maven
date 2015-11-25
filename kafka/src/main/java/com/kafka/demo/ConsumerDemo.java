package com.kafka.demo;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Created by xianwei on 2015/11/25.
 *
 */
public class ConsumerDemo {
    private static final String topic = "workcount";
    private static final Integer threads=1;

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "hadoop04:2181,hadoop05:2181,hadoop06:2181");
        properties.put("group.id","1111");
        properties.put("auto.offset.reset","smallest");

        ConsumerConfig config = new ConsumerConfig(properties);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);

        Map<String,Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        topicCountMap.put("mygirls", 1);
        topicCountMap.put("myboys", 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("workcount");

        for(final KafkaStream<byte[], byte[]> kafkaStream : streams){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for(MessageAndMetadata<byte[], byte[]> mm : kafkaStream){
                        String msg = new String(mm.message());
                        System.out.println(msg);
                    }
                }

            }).start();

        }

    }
}
