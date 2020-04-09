package com.tuan.sl.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerWrapper {

    private static Properties consumerConfig(String clientId, String consumerStrategy) {
        /* 定义kakfa 服务的地址，不需要将所有broker指定上 */
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "106.75.218.68:9092");
        /* 是否自动确认offset */
        properties.put("enable.auto.commit", "true");
        properties.put("max.poll.records", "10");
        /* 自动确认offset的时间间隔 */
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("max.poll.interval.ms", "900000");
        properties.put("client.id", clientId);
        properties.put("auto.offset.reset", consumerStrategy); // 必须加
        /* key的序列化类 */
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        /* value的序列化类 */
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }
    public static void main(String[] args) {
        Properties properties = consumerConfig("data-pipeline","earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        List<String> topics = new ArrayList<>();
        topics.add("ecarx-data-pipeline");
        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
