package com.maxplus1.hd_client.hbase.operations.client;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * wrap column definition for schema
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ColumnMetaData {
    private String tableName;
    private String columnFamily;
    private String columnName;

    public boolean hasColumnFamily(){
        return getColumnFamily()!=null&&getColumnFamily().length()>0;
    }
}
