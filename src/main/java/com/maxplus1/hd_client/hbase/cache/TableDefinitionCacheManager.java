package com.maxplus1.hd_client.hbase.cache;

import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans.ColumnDefinition;
import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans.RowkeyDefinition;
import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans.TableDefinition;
import com.maxplus1.hd_client.hbase.operations.client.ColumnMetaData;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by qxloo on 2016/12/21.
 */
public class TableDefinitionCacheManager {

    /**
     * 缓存这类注解的解析结果：TableDefinition。减少反射消耗
     */
    private final static ConcurrentHashMap<Class,TableDefinition> COLUMN_DEFINITION_CACHE_MAP = new ConcurrentHashMap<>();

    public static void put(Class clz,TableDefinition tableDefinition){
        COLUMN_DEFINITION_CACHE_MAP.put(clz,tableDefinition);
    }

    public static TableDefinition get(Class clz){
        return COLUMN_DEFINITION_CACHE_MAP.get(clz);
    }


    /**
     * 注意此处的递归调用
     * @param query
     * @param classType
     * @param isInRecursion
     * @param <T>
     * @return
     */
    public static  <T extends Query>  T buildQuery(T query, Class classType, boolean isInRecursion){
        TableDefinition tableDefinition = TableDefinitionCacheManager.get(classType);
        RowkeyDefinition rowkeyDefinition = tableDefinition.getRowkeyDefinition();
        if(isInRecursion){
            if(rowkeyDefinition.isHasRoweyAndColumnAnno()){//具有column注解，当做普通字段处理
                if (rowkeyDefinition.hasColumnFamily()) {
                    addColumn(query,rowkeyDefinition);
                } else {
                    buildQuery(query,rowkeyDefinition.getFieldType(),true);
                }
            }
        }
        List<ColumnDefinition> columnDefinitions = tableDefinition.getColumnDefinitionList();
        for (ColumnDefinition columnDefinition : columnDefinitions) {
            if(columnDefinition.hasColumnFamily()){
                addColumn(query,columnDefinition);
            }else{
                buildQuery(query,columnDefinition.getFieldType(),true);
            }
        }
        return query;
    }

    private static <T extends Query> void addColumn(T query,ColumnMetaData columnMetaData){
        if(query instanceof Scan){
            ((Scan)query).addColumn(Bytes.toBytes(columnMetaData.getColumnFamily()),Bytes.toBytes(columnMetaData.getColumnName()));
        }else if (query instanceof Get){
            ((Get)query).addColumn(Bytes.toBytes(columnMetaData.getColumnFamily()),Bytes.toBytes(columnMetaData.getColumnName()));
        }else {
            //TODO: Unexpected Type.it must be Get or Scan
        }
    }

}
