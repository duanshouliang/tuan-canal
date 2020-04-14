package com.tuan.sl.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;

public class CanalConnectorWrapper {

    private String instance;
    private String kafkaTopic;
    private CanalConnector canalConnector;


    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public CanalConnector getCanalConnector() {
        return canalConnector;
    }

    public void setCanalConnector(CanalConnector canalConnector) {
        this.canalConnector = canalConnector;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }
}
