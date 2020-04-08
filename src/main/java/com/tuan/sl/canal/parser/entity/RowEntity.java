package com.tuan.sl.canal.parser.entity;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class RowEntity implements Serializable {

    private String instanceName;
    private String schemaName;
    private String tableName;
    private String eventType;
    private Map<String, Object> newRow;
    private Map<String, Object> oldRow;
    private Date operateTime;
    private List<String> primaryKeys;

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Map<String, Object> getNewRow() {
        return newRow;
    }

    public void setNewRow(Map<String, Object> newRow) {
        this.newRow = newRow;
    }

    public Map<String, Object> getOldRow() {
        return oldRow;
    }

    public void setOldRow(Map<String, Object> oldRow) {
        this.oldRow = oldRow;
    }

    public Date getOperateTime() {
        return operateTime;
    }

    public void setOperateTime(Date operateTime) {
        this.operateTime = operateTime;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }
}
