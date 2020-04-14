package com.tuan.sl.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;


@Component("kafkaProducerWrapper")
public class KafkaProducerWrapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerWrapper.class);

    private KafkaProducer<String, String> kafkaProducer;

    @Autowired
    private KafkaContext kafkaContext;

    public KafkaProducerWrapper() {

    }

    @PostConstruct
    public void init(){
        kafkaProducer = new KafkaProducer<String, String>(kafkaContext.getProperties());
    }

    public void send(String topic, Integer partition, String data){
        kafkaProducer.send(new ProducerRecord<>(topic, partition, null, data), (recordMetadata, e) -> {
            if (e != null) {
                LOGGER.error("Send data to kafka " + recordMetadata + " error: ", e);
            }
        });
    }

    public void send(String topic, String data){
        kafkaProducer.send(new ProducerRecord<>(topic, null, data), (recordMetadata, e) -> {
            if (e != null) {
                LOGGER.error("Send data to kafka " + recordMetadata + " error: ", e);
            }
        });
    }
}
