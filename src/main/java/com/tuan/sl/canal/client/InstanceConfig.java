package com.tuan.sl.canal.client;

import lombok.Data;

import java.io.Serializable;

@Data
public class InstanceConfig implements Serializable{
    private String server;
    private String instance;
    private Integer port;
    private String username;
    private String password;
    private String kafkaTopic;
}
