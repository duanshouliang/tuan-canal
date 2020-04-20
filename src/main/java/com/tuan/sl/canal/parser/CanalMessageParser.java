package com.tuan.sl.canal.parser;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.Message;
import com.tuan.sl.canal.parser.entity.RowEntity;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class CanalMessageParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalMessageParser.class);

    public static List<RowEntity> parser(Message message, String instance){
        List<RowEntity> rowEntities = new ArrayList<>();
        List<Entry> entries = message.getEntries();
        for(Entry entry : entries){
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                LOGGER.error("Parse canal row data with exception {}, stack {}, data {}", rowChage != null ? JSON.toJSONString(rowChage) : "none");
                continue;
            }

            if(null == rowChage){
                continue;
            }
            List<CanalEntry.RowData> rowDataList = rowChage.getRowDatasList();
            if(null == rowDataList || rowDataList.size() == 0){
                continue;
            }
            CanalEntry.EventType eventType = rowChage.getEventType();
            String schemaName = entry.getHeader().getSchemaName().toLowerCase();
            String tableName = entry.getHeader().getTableName().toLowerCase();
            String type = eventType.getValueDescriptor().getName().toLowerCase();
            Date operateTime = new Date();
            for(CanalEntry.RowData rowData : rowDataList){
                RowEntity rowEntity = new RowEntity();
                rowEntity.setInstanceName(instance);
                rowEntity.setSchemaName(schemaName);
                rowEntity.setTableName(tableName);
                rowEntity.setEventType(type);
                rowEntity.setOperateTime(operateTime);
                rowEntity.setPrimaryKeys(new ArrayList<>());
                if(eventType == CanalEntry.EventType.DELETE){
                    Map<String, Object> row = getRowColumn(rowData.getBeforeColumnsList(), rowEntity, eventType);
                    rowEntity.setOldRow(row);
                }else if(eventType == CanalEntry.EventType.INSERT){
                    Map<String, Object> row = getRowColumn(rowData.getAfterColumnsList(), rowEntity, eventType);
                    rowEntity.setNewRow(row);
                }else{
                    Map<String, Object> newData = getRowColumn(rowData.getAfterColumnsList(), rowEntity, eventType);
                    rowEntity.setNewRow(newData);
                    Map<String, Object> oldData = getRowColumn(rowData.getBeforeColumnsList(), rowEntity, null);
                    rowEntity.setOldRow(oldData);
                }
                rowEntities.add(rowEntity);
            }
        }

        return rowEntities;

    }

    public static Map<String, Object> getRowColumn(List<CanalEntry.Column> columnList, RowEntity rowEntity, CanalEntry.EventType eventType){
        Map<String, Object> columns = new HashMap<>();
        for(CanalEntry.Column column : columnList){
            if(eventType == CanalEntry.EventType.UPDATE ){
                if(column.getUpdated()) {
                    columns.put(column.getName(), parseValueByMysqlType(column.getValue(), column.getMysqlType()));
                }
            }else {
                columns.put(column.getName(), parseValueByMysqlType(column.getValue(), column.getMysqlType()));
                if (column.getIsKey()) {
                    rowEntity.getPrimaryKeys().add(column.getName());
                }
            }
        }
        return columns;
    }

    private static Object parseValueByMysqlType(String value, String mysqlType) {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        try {
            if (mysqlType.indexOf("bigint") > -1) {
                return Long.parseLong(value);
            } else if (mysqlType.indexOf("int") > -1) {
                return Integer.parseInt(value);
            } else if (mysqlType.indexOf("double") > -1) {
                return Double.parseDouble(value);
            } else if (mysqlType.indexOf("float") > -1) {
                return Float.parseFloat(value);
            } else if (mysqlType.indexOf("datetime") > -1) {
                return dateParser("yyyy-MM-dd HH:mm:ss", value);
            } else if (mysqlType.indexOf("date") > -1) {
                return dateParser("yyyy-MM-dd",value);
            } else {
                return value;
            }
        } catch (Exception e) {
            LOGGER.error("Parse data value error ", e);
            return value;
        }
    }


    private static Date dateParser(String pattern, String date){
        DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern(pattern);
        LocalDateTime localDateTime = LocalDateTime.parse(date, dtf2);
        Instant istant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
        return Date.from(istant);
    }
}
