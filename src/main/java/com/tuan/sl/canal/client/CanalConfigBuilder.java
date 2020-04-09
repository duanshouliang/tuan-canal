package com.tuan.sl.canal.client;

import java.util.ArrayList;
import java.util.List;

public class CanalConfigBuilder {
    private List<CanalConfig> canalConfigs = new ArrayList<>();

    public List<CanalConfig> build(String server, Integer port, String instance, String username, String password){
        CanalConfig config = new CanalConfig();
        config.setServer(server);
        config.setPort(port);
        config.setInstance(instance);
        config.setUsername(username);
        config.setPassword(password);
        canalConfigs.add(config);
        return canalConfigs;
    }

    public List<CanalConfig> getConfig(){
        return canalConfigs;
    }
}
