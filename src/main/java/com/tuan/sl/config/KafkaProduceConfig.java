package com.tuan.sl.config;

import com.tuan.sl.kafka.KafkaContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaProduceConfig {
    private  KafkaContext kafkaContext;

    @Value("${spring.kafka.servers}")
    private String servers;

    @Bean
    public KafkaContext buildContext(){
        kafkaContext = KafkaContext.getInstance();
        kafkaContext.addConfig("bootstrap.servers", servers);
        kafkaContext.addConfig("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaContext.addConfig("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return kafkaContext;
    }
}
