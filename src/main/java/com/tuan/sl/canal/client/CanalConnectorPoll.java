package com.tuan.sl.canal.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;

public class CanalConnectorPoll {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalConnectorPoll.class);

    private List<CanalConfig> canalonfigs;
    private List<CanalConnectorWrapper> connectors;


    public CanalConnectorPoll(List<CanalConfig> canalonfigs) {
        this.canalonfigs = canalonfigs;
        this.buildConnectors();
    }

    private void buildConnectors() {
        if (null == canalonfigs || canalonfigs.size() == 0) {
            return;
        }
        connectors = new ArrayList<>();
        canalonfigs.forEach(config -> {
            CanalConnectorWrapper wrapper = this.build(config);
            if (null != wrapper) {
                connectors.add( wrapper);
            }
        });
    }

    private CanalConnectorWrapper build(CanalConfig connectorConfig) {
        CanalConnectorWrapper connectorWrapper = null;
        try {
            String server = connectorConfig.getServer();
            Integer port = connectorConfig.getPort();
            String instance = connectorConfig.getInstance();
            String username = connectorConfig.getUsername();
            String password = connectorConfig.getPassword();
            CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(server, port), instance, username, password);
            if (null != connector) {
                connector.connect();
                connector.subscribe();
                connector.rollback();
                connectorWrapper = new CanalConnectorWrapper();
                connectorWrapper.setCanalConnector(connector);
                connectorWrapper.setInstance(instance);
                LOGGER.info("Connect to canal instance" + server + " " + port + " " + instance);
            } else {
                LOGGER.info("Connect to canal instance" + server + " " + port + " " + instance + " failed");
            }
        } catch (Exception e) {
            LOGGER.info("Connect to canal instance" + JSON.toJSONString(connectorConfig) + " failed");
            LOGGER.error("Get canal connector with exception {}, stack {}", e.getMessage(), Arrays.toString(e.getStackTrace()));
        }
        return connectorWrapper;
    }

    public List<CanalConnectorWrapper> getConnectors() {
        return connectors;
    }
}
