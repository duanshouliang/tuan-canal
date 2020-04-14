package com.tuan.sl.canal.executor;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.tuan.sl.canal.client.CanalConnectorWrapper;
import com.tuan.sl.canal.parser.CanalMessageParser;
import com.tuan.sl.canal.parser.entity.RowEntity;
import com.tuan.sl.kafka.KafkaProducerWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DataPipelineExecutor implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataPipelineExecutor.class);
    private static final Integer DEFAULT_BATCH_SIZE = 100;
    private static final Integer TIMEOUT = 100;
    private volatile boolean running = true;
    private CanalConnectorWrapper canalConnectorWrapper;
    private KafkaProducerWrapper kafkaProducerWrapper;
    private CountDownLatch countDownLatch;

    public DataPipelineExecutor(){}

    public DataPipelineExecutor(CanalConnectorWrapper canalConnectorWrapper, KafkaProducerWrapper kafkaProducerWrapper ){
        this.canalConnectorWrapper = canalConnectorWrapper;
        this.kafkaProducerWrapper = kafkaProducerWrapper;
    }
    @Override
    public void run() {
        CanalConnector connector = canalConnectorWrapper.getCanalConnector();
        String instance = canalConnectorWrapper.getInstance();
        String topic = canalConnectorWrapper.getKafkaTopic();
        while (running){
            Message message;
            try {
                message = connector.getWithoutAck(DEFAULT_BATCH_SIZE);
            }catch (Exception e){
                LOGGER.error("Get data by canal of instance {} with exception {}, stack {}", instance, e.getMessage(), Arrays.toString(e.getStackTrace()));
                continue;
            }
            if(null == message){
                continue;
            }
            Long batchId = message.getId();
            int size = message.getEntries().size();
            if (batchId == -1 || size == 0) {
                // ack
                connector.ack(batchId);
                LOGGER.info(String.format("%s not update", instance));
                try {
                    Thread.sleep(TIMEOUT);
                } catch (InterruptedException e) {
                    LOGGER.info("Canal consumer thread has interrupted");
                    Thread.currentThread().interrupt();
                }
            }else {
                //parse massage
                List<RowEntity> rowEntities = CanalMessageParser.parser(message, instance);
                for(RowEntity rowEntity : rowEntities){
                    String data = JSONObject.toJSONString(rowEntity, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteDateUseDateFormat);
                    LOGGER.info("Received data from canal" + ": " + data);
                    kafkaProducerWrapper.send(topic, data);
                }
                connector.ack(batchId);
            }
        }
        if(null != connector){
            connector.disconnect();
        }
        countDownLatch.countDown();
        LOGGER.info("Instance " + instance + "disconnect from canal");
    }

    public void stop(CountDownLatch countDownLatch){
        this.countDownLatch = countDownLatch;
        running  = false;
    }
}
