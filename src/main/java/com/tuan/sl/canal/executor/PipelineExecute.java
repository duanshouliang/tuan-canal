package com.tuan.sl.canal.executor;

import com.tuan.sl.canal.client.CanalConnectorPoll;
import com.tuan.sl.canal.client.CanalConnectorWrapper;
import com.tuan.sl.canal.client.CanalContext;
import com.tuan.sl.kafka.KafkaProducerWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component("pipelineExecute")
public class PipelineExecute {
    @Autowired
    private CanalContext canalContext;

    @Autowired
    private KafkaProducerWrapper kafkaProducer;

    public PipelineExecute(){
    }

    @PostConstruct
    public void execute(){
        CanalConnectorPoll poll = new CanalConnectorPoll(canalContext.getInstanceConfigs());
        List<CanalConnectorWrapper> connectors  = poll.getConnectors();
        if(null == connectors || connectors.size() == 0){
            return;
        }
        ExecutorService executor = Executors.newFixedThreadPool(connectors.size());
        connectors.forEach(connector -> {
            executor.submit(new DataPipelineExecutor(connector, kafkaProducer));
        });
    }
}
