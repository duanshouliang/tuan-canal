package com.tuan.sl.canal.executor;

import com.tuan.sl.canal.client.CanalContext;
import com.tuan.sl.canal.client.MysqlInstanceMonitor;
import com.tuan.sl.kafka.KafkaProducerWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;

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
        MysqlInstanceMonitor instanceMonitor = new MysqlInstanceMonitor(kafkaProducer, canalContext);
        instanceMonitor.init();
    }
}
