package com.maxplus1.hd_client.hbase.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by qxloo on 2016/11/30.
 */
@Slf4j
public enum PutUtils {
    ;
    public static boolean put(Table table, Put put) {
        return TableTemplate.opt(table, new OptCallback<Boolean>() {
            @Override
            public void doInTable(Table table) throws IOException {
                table.put(put);
            }
        });
    }

    public static boolean put(Table table, List<Put> puts) {
        return TableTemplate.opt(table, new OptCallback<Boolean>() {
            @Override
            public void doInTable(Table table) throws IOException {
                table.put(puts);
            }
        });
    }

}
