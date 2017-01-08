package com.maxplus1.hd_client.hbase.cache;

import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.beans.ColumnTypeMetaData;
import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.beans.ColumnMetaData;
import com.maxplus1.hd_client.hbase.operations.client.rtn_map.beans.TableColumn;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by qxloo on 2016/11/27.
 */
@Deprecated
public class TableColumnCacheManager {
    /**
     * k:表明
     * v:table column
     */
    private final static ConcurrentHashMap<ColumnMetaData,ColumnTypeMetaData.ColumnType> TABLE_COLUMN_CACHE_MAP = new ConcurrentHashMap<>();

    public static void put(ColumnMetaData columnMetaData,ColumnTypeMetaData.ColumnType columnType){
        TABLE_COLUMN_CACHE_MAP.put(columnMetaData,columnType);
    }

    public static void put(String tableName,String family,String column,ColumnTypeMetaData.ColumnType columnType){
        ColumnMetaData columnMetaData = new ColumnMetaData();
        columnMetaData.setTableName(tableName);
        columnMetaData.setColumnFamily(family);
        columnMetaData.setColumnName(column);
        TABLE_COLUMN_CACHE_MAP.put(columnMetaData,columnType);
    }

    public static ColumnTypeMetaData.ColumnType get(ColumnMetaData columnMetaData){
        return TABLE_COLUMN_CACHE_MAP.get(columnMetaData);
    }

    public static ColumnTypeMetaData.ColumnType get(String tableName, String family, String column){
        return TABLE_COLUMN_CACHE_MAP.get(new ColumnMetaData(tableName,family,column));
    }

    public static void init(ConcurrentHashMap<ColumnMetaData,ColumnTypeMetaData.ColumnType> map){
        TABLE_COLUMN_CACHE_MAP.clear();
        TABLE_COLUMN_CACHE_MAP.putAll(map);
    }

}
