package com.tuan.sl.config;

import com.tuan.sl.canal.client.CanalContext;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;

@Configuration
public class CanalInstanceConfigs {
    private CanalContext canalContext;

    @Value("${spring.canal.instances}")
    private String instanceConfigs;

    @Value("${spring.canal.instance_topic}")
    private String instanceTopics;



    @Bean
    public CanalContext buildCanalContext() {
        canalContext = CanalContext.getCanalContext();
        if(StringUtils.isNotBlank(instanceConfigs)){
            List<String> configs = Arrays.asList(instanceConfigs.split(";"));
            canalContext.buildContext(configs);
        }

        if(StringUtils.isNotBlank(instanceTopics)){
            List<String> instanceTopicList = Arrays.asList(instanceTopics.split(";"));
            canalContext.buildInstanceTopicContext(instanceTopicList);
        }
        return canalContext;
    }
}
