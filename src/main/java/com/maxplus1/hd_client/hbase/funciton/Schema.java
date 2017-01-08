package com.maxplus1.hd_client.hbase.funciton;

import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * 提供HBase schema的操作功能
 * 
 * @author zachary.zhang
 * @author Paleo
 */
public interface Schema {

    void createTable(String tableName, String... columnFamily) ;

    void deleteTable(String tableName) ;

    HTableDescriptor[]  listTables() ;

    HTableDescriptor[]  listTables(String regex) ;
}
