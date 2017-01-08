package com.maxplus1.hd_client.hbase.cache;

import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans.ColumnDefinition;
import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans.RowkeyDefinition;
import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans.TableDefinition;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by qxloo on 2016/12/21.
 */
public class TableDefinitionCacheManager {

    private final static ConcurrentHashMap<Class,TableDefinition> COLUMN_DEFINITION_CACHE_MAP = new ConcurrentHashMap<>();

    public static void put(Class clz,TableDefinition tableDefinition){
        COLUMN_DEFINITION_CACHE_MAP.put(clz,tableDefinition);
    }

    public static TableDefinition get(Class clz){
        return COLUMN_DEFINITION_CACHE_MAP.get(clz);
    }

    public static Scan buildScan(Scan scan,Class classType,boolean isInRecursion){
        TableDefinition tableDefinition = TableDefinitionCacheManager.get(classType);
        RowkeyDefinition rowkeyDefinition = tableDefinition.getRowkeyDefinition();
        if(isInRecursion){
            if(rowkeyDefinition.isHasRoweyAndColumnAnno()){//具有column注解，当做普通字段处理
                if (rowkeyDefinition.hasColumnFamily()) {
                    scan.addColumn(Bytes.toBytes(rowkeyDefinition.getColumnFamily()),Bytes.toBytes(rowkeyDefinition.getColumnName()));
                } else {
                    buildScan(scan,rowkeyDefinition.getFieldType(),true);
                }
            }
        }
        List<ColumnDefinition> columnDefinitions = tableDefinition.getColumnDefinitionList();
        for (ColumnDefinition columnDefinition : columnDefinitions) {
            if(columnDefinition.hasColumnFamily()){
                scan.addColumn(Bytes.toBytes(columnDefinition.getColumnFamily()),Bytes.toBytes(columnDefinition.getColumnName()));
            }else{
                buildScan(scan,columnDefinition.getFieldType(),true);
            }
        }
        return scan;
    }

    public static Get buildGet(Get get, Class classType,boolean isInRecursion){
        TableDefinition tableDefinition = TableDefinitionCacheManager.get(classType);
        RowkeyDefinition rowkeyDefinition = tableDefinition.getRowkeyDefinition();
        if(isInRecursion){
            if(rowkeyDefinition.isHasRoweyAndColumnAnno()){//具有column注解，当做普通字段处理
                if (rowkeyDefinition.hasColumnFamily()) {
                    get.addColumn(Bytes.toBytes(rowkeyDefinition.getColumnFamily()),Bytes.toBytes(rowkeyDefinition.getColumnName()));
                } else {
                    buildGet(get,rowkeyDefinition.getFieldType(),true);
                }
            }
        }
        List<ColumnDefinition> columnDefinitions = tableDefinition.getColumnDefinitionList();
        for (ColumnDefinition columnDefinition : columnDefinitions) {
            if(columnDefinition.hasColumnFamily()){
                get.addColumn(Bytes.toBytes(columnDefinition.getColumnFamily()),Bytes.toBytes(columnDefinition.getColumnName()));
            }else{
                buildGet(get,columnDefinition.getFieldType(),true);
            }
        }
        return get;
    }

}
