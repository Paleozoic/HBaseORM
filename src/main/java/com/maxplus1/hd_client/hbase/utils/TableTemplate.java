/*
package com.maxplus1.hd_client.hbase.utils;

import com.maxplus1.hd_client.hbase.exception.HbaseClientException;
import com.maxplus1.hd_client.hbase.operations.client.TableCallback;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

*/
/**
 * Created by qxloo on 2017/5/28.
 *//*

@Slf4j
public class TableTemplate {

    public static <T> T opt(Table table,TableCallback<T> tableCallback){
        try {
            return tableCallback.doInTable(table);
        } catch (Throwable e) {
            log.error("[ERROR===>>>]"+e.getMessage(), e);
            throw new HbaseClientException(e);
        } finally {
            releaseTable(table);
        }
    }

    private static void releaseTable(Table table)  {
        if(table!=null){
            try {
                table.close();
            } catch (IOException e) {
                log.error("[ERROR===>>>]close table error!", e);
                throw new HbaseClientException(e);
            }
        }
    }

}
*/
