package com.tuan.sl.config;

import com.tuan.sl.DataPipelineApp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(properties = "spring.profiles.active=local", classes = DataPipelineApp.class)
@RunWith(SpringRunner.class)
public class CanalInstanceConfigsTest {

    @Autowired
    private KafkaProduceConfig kafkaConfig;

    @Test
    public void getInstanceConfigs() {
//        String servers  = kafkaConfig.getServers();
//
//        System.out.println("xxx");
    }
}