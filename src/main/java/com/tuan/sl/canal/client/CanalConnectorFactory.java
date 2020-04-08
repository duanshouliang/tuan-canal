package com.tuan.sl.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;

public class CanalConnectorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalConnectorFactory.class);

    private String server = "127.0.0.1";
    private String instance = "example";
    private Integer port = 11111;

    private CanalConnector connector;

    public CanalConnector getConnector(){
        this.createConnector();
        return connector;
    }
    private void createConnector(){
        try {

            connector = CanalConnectors.newSingleConnector(new InetSocketAddress(server, port), instance, "", "");
            if(null != connector){
                connector.connect();
                connector.subscribe();
                connector.rollback();
                LOGGER.info("Connect to canal instance" + server + " " +port + " " + instance);
            }else{
                LOGGER.info("Connect to canal instance" + server + " " +port + " " + instance + " failed");
            }
        }catch (Exception e){
            LOGGER.info("Connect to canal instance" + server + " " +port + " " + instance + " failed");
            LOGGER.error("Get canal connector with exception {}, stack {}", e.getMessage(), Arrays.toString(e.getStackTrace()));
        }
    }

    public String getInstance() {
        return instance;
    }
}
