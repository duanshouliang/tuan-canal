package com.tuan.sl.canal.executor;

import com.tuan.sl.canal.client.CanalConfig;
import com.tuan.sl.canal.client.CanalConnectorPoll;
import com.tuan.sl.canal.client.CanalConnectorWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataPipelineExecute {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataPipelineExecute.class);

    private CanalConnectorPoll poll;

    public DataPipelineExecute(List<CanalConfig> configs){
        poll = new CanalConnectorPoll(configs);
    }
    public void execute(){
        List<CanalConnectorWrapper> connectors  = poll.getConnectors();
        if(null == connectors || connectors.size() == 0){
            return;
        }
        ExecutorService executor = Executors.newFixedThreadPool(connectors.size());
        connectors.forEach(connector -> {
            executor.submit(new DataPipelineExecutor(connector));
        });
    }
}
