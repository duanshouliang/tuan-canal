package com.tuan.sl.canal.executor;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;
import com.tuan.sl.canal.client.CanalConnectorFactory;
import com.tuan.sl.canal.parser.CanalMessageParser;
import com.tuan.sl.canal.parser.entity.RowEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class CanalExecutor implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalExecutor.class);
    private static final Integer DEFAULT_BATCH_SIZE = 100;
    private static final Integer TIMEOUT = 100;
    private volatile boolean running = true;
    private CanalConnectorFactory canalConnectorFactory;


    public CanalExecutor(){}

    public CanalExecutor(CanalConnectorFactory canalConnectorFactory){
        this.canalConnectorFactory = canalConnectorFactory;
    }
    @Override
    public void run() {
        CanalConnector connector = canalConnectorFactory.getConnector();
        String instance = canalConnectorFactory.getInstance();
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


                }
                // ack
                connector.ack(batchId);
            }

        }
    }
}
