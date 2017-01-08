package com.maxplus1.hd_client.hbase.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by qxloo on 2016/11/30.
 */
@Slf4j
public enum DelUtils {
    ;
    public static boolean delete(Table table,String rowkey) {
        try {
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            table.delete(delete);
            return true;
        } catch (IOException e) {
            log.error("[ERROR===>>>]delete the rowkey ["+rowkey+"] error!", e);
            return false;
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                log.error("[ERROR===>>>]close table error!", e);
            }
        }
    }

    public static boolean deleteList(Table table,List<String> rowkeyList) {
        try {
            List<Delete> list = new ArrayList<>();
            for (String rowkey : rowkeyList) {
                Delete delete = new Delete(Bytes.toBytes(rowkey));
                list.add(delete);
            }
            table.delete(list);
            return true;
        } catch (IOException e) {
            log.error("[ERROR===>>>]delete rowkey list error!", e);
            return false;
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                log.error("[ERROR===>>>]close table error!", e);
            }
        }
    }

}
