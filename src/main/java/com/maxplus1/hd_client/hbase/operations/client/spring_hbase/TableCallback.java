package com.maxplus1.hd_client.hbase.operations.client.spring_hbase;

import org.apache.hadoop.hbase.client.Table;

/**
 * Created by qxloo on 2016/12/2.
 */
public interface TableCallback<T> {


    T doInTable(Table table) throws Throwable;
}