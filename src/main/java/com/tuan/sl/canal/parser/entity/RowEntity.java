package com.tuan.sl.canal.parser.entity;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
public class RowEntity implements Serializable {
    private String instanceName;
    private String schemaName;
    private String tableName;
    private String eventType;
    private Map<String, Object> newRow;
    private Map<String, Object> oldRow;
    private Date operateTime;
    private List<String> primaryKeys;
}
