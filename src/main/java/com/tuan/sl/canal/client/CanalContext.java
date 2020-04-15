package com.tuan.sl.canal.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CanalContext {
    private static CanalContext canalContext = null;
    private List<InstanceConfig> instanceConfigs = new ArrayList<>();
    private Map<String, String> instanceTopics = new HashMap<>();

    private CanalContext(){}

    public static CanalContext getCanalContext(){
        if(canalContext == null){
            synchronized (CanalContext.class){
                if(canalContext == null){
                    canalContext = new CanalContext();
                }
            }
        }
        return canalContext;
    }

    public void buildContext(List<String> instances){
        if(null == instances || instances.size() == 0)return;
        instances.forEach(instance ->{
            String[] items = instance.split(":");
            InstanceConfig config = new InstanceConfig();
            config.setServer(items[0]);
            config.setPort(Integer.parseInt(items[1]));
            config.setInstance(items[2]);
            config.setKafkaTopic(items[3]);
            config.setUsername("");
            config.setPassword("");
            instanceConfigs.add(config);
        });
    }

    public void buildInstanceTopicContext(List<String> instanceTopicList){
        if(null == instanceTopicList || instanceTopicList.size() == 0)return;
        instanceTopicList.forEach(instanceTopic ->{
            String[] items = instanceTopic.split(":");
            instanceTopics.put(items[0], items[1]);
        });
    }

    public List<InstanceConfig> getInstanceConfigs() {
        return instanceConfigs;
    }

    public Map<String, String> getInstanceTopics() {
        return instanceTopics;
    }
}
