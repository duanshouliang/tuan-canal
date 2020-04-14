package com.tuan.sl.canal.client;

import java.util.ArrayList;
import java.util.List;

public class CanalConfigBuilder {
    private List<InstanceConfig> instanceConfigs = new ArrayList<>();

    public List<InstanceConfig> build(String server, Integer port, String instance, String username, String password){
        InstanceConfig config = new InstanceConfig();
        config.setServer(server);
        config.setPort(port);
        config.setInstance(instance);
        config.setUsername(username);
        config.setPassword(password);
        instanceConfigs.add(config);
        return instanceConfigs;
    }

    public List<InstanceConfig> getConfig(){
        return instanceConfigs;
    }
}
