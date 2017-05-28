package com.maxplus1.hd_client.hbase.utils;


import org.apache.hadoop.hbase.client.Table;

/**
 * Created by qxloo on 2017/5/28.
 */
public interface OptCallback<T> {
    void doInTable(Table table) throws Throwable;
}
