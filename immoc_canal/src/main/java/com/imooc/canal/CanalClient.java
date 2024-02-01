package com.imooc.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.base.CaseFormat;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) throws Exception {

        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("ruozedata001", 11111), "example", null, null
        );

        while(true) {
            connector.connect();

            connector.subscribe("pkdb.*");
            Message message = connector.get(100);

            List<CanalEntry.Entry> entries = message.getEntries();
            if(!entries.isEmpty()) {
                for (CanalEntry.Entry entry : entries) {
                    String tableName = entry.getHeader().getTableName();

                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();

                    // insert update delete...
                    CanalEntry.EventType eventType = rowChange.getEventType();

                    if(eventType == CanalEntry.EventType.INSERT) {
                        for (CanalEntry.RowData rowData: rowDataList) {
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                            HashMap<String, String> map = new HashMap<>();

                            for(CanalEntry.Column column : afterColumnsList) {
                                String key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                                map.put(key, column.getValue());
                            }

                            System.out.println("tableName" + tableName + " , " + JSON.toJSONString(map));
                        }
                    }
                }
            }
        }
    }
}
