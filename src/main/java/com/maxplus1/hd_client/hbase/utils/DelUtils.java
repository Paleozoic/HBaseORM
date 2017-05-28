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

    /**
     * 根据rowkey删除Table的一行数据
     * @param table
     * @param rowkey
     * @return
     */
    public static boolean delete(Table table,String rowkey) {
        return TableTemplate.opt(table, new OptCallback<Boolean>() {
            @Override
            public void doInTable(Table table) throws IOException {
                Delete delete = new Delete(Bytes.toBytes(rowkey));
                table.delete(delete);
            }
        });
    }

    /**
     * 根据rowkeyList删除Table的多行数据
     * @param table
     * @param rowkeyList
     * @return
     */
    public static boolean deleteList(Table table,List<String> rowkeyList) {
        return TableTemplate.opt(table, new OptCallback<Boolean>() {
            @Override
            public void doInTable(Table table) throws IOException {
                List<Delete> list = new ArrayList<>();
                for (String rowkey : rowkeyList) {
                    Delete delete = new Delete(Bytes.toBytes(rowkey));
                    list.add(delete);
                }
                table.delete(list);
            }
        });
    }

}
