package com.tuan.sl.kafka;

import java.util.Properties;

public class KafkaContext {
    private static KafkaContext kafkaContext;
    private Properties properties = new Properties();

    private KafkaContext(){
    }

    public static KafkaContext getInstance(){
        if(null == kafkaContext){
            synchronized (KafkaContext.class){
                if(null == kafkaContext){
                    kafkaContext = new KafkaContext();
                }
            }
        }
        return kafkaContext;
    }

    public void addConfig(String key, Object value){
        properties.put(key, value);
    }

    public Properties getProperties() {
        return properties;
    }
}
