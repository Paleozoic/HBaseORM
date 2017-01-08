package com.maxplus1.hd_client.hbase.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.List;

/**
 * Created by qxloo on 2016/11/30.
 */
@Slf4j
public enum PutUtils {
    ;
    public static boolean put(Table table, Put put) {
        try {
            table.put(put);
        } catch (IOException e) {
            log.error("[ERROR===>>>]", e);
            return false;
        } finally {
            try {
                if(table!=null){
                    table.close();
                }
            } catch (IOException e) {
                log.error("[ERROR===>>>]close table error!]", e);
            }
        }
        return true;
    }

    public static boolean put(Table table, List<Put> puts) {
        try {
            table.put(puts);
        } catch (IOException e) {
            log.error("[ERROR===>>>]", e);
            return false;
        } finally {
            try {
                if(table!=null){
                    table.close();
                }
            } catch (IOException e) {
                log.error("[ERROR===>>>]close table error!]", e);
            }
        }
        return true;
    }

}
