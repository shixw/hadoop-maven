package com.storm.kafka.demo.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by xianwei on 2015/11/25.
 *
 */
public class PropertyUtil {
    private static Properties properties = new Properties();

    static {
        try {
            InputStream in = PropertyUtil.class.getClassLoader().getResourceAsStream("config.properties");
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}
