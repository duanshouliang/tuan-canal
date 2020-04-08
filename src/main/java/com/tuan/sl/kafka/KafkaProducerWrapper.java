package com.tuan.sl.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerWrapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerWrapper.class);

    private final KafkaProducer<String, String> kafkaProducer;

    public KafkaProducerWrapper() {
        Properties kafkaConfig = new Properties();
        kafkaConfig.put("bootstrap.servers", "10.171.31.87:9092,10.171.31.53:9092,10.171.31.203:9092");
        kafkaConfig.put("client.id", "data-pipeline");
        kafkaConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaProducer = new KafkaProducer<String, String>(kafkaConfig);
    }

    public void send(String topic, Integer partition, String data){
        kafkaProducer.send(new ProducerRecord<>(topic, partition, null, data), (recordMetadata, e) -> {
            if (e != null) {
                LOGGER.error("Send data to kafka " + recordMetadata + " error: ", e);
            }
        });
    }

    public static void main(String[] args) {
        KafkaProducerWrapper producerWrapper = new KafkaProducerWrapper();
        producerWrapper.send("ecarx-data-pipeline",0,"xxxxxxxx duanshuliang ");
    }
}
