package com.storm.kafka.demo.spout;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by xianwei on 2015/11/25.
 *
 */
public class MessageSpout implements Scheme {

    private static final long serialVersionUID = -5888309493758923245L;

    @Override
    public List<Object> deserialize(byte[] bytes) {
        try {
            String msg = new String(bytes,"UTF-8");
            return new Values(msg);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("msg");
    }
}
