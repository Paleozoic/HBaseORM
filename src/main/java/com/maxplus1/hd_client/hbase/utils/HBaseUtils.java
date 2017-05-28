package com.maxplus1.hd_client.hbase.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.maxplus1.hd_client.hbase.annotation.Table;
import com.maxplus1.hd_client.hbase.exception.HbaseClientException;
import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans.ColumnDefinition;
import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans.RowkeyDefinition;
import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans.TableDefinition;
import com.maxplus1.hd_client.hbase.type.resolvers.TypeResolver;
import com.maxplus1.hd_client.hbase.type.rtn_pojo.TypeResolverFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * operations utils class , apply some generic functions
 * 
 * @author zachary.zhang
 * @author edit by Paleo
 */
@Slf4j
public enum HBaseUtils {
    ;
    public static Put wrapPut(Object po) throws IllegalAccessException {

        Preconditions.checkNotNull(po, "[ERROR===>>>]persistent object can't be null");
        return wrapPut(po,null);
    }

    public static Put wrapPut(Object po,String rowkey) throws IllegalAccessException {
        Preconditions.checkNotNull(po, "[ERROR===>>>]persistent object can't be null");
        return convertPut(null,po);
    }

    public static <T> List<Put> wrapPutList(List<T> poList) throws IllegalAccessException {
        Preconditions.checkNotNull(poList, "[ERROR===>>>]persistent object list can't be null");
        Preconditions.checkArgument(poList.size() != 0, "[ERROR===>>>]persistent object list can't be empty");

        List<Put> puts = Lists.newArrayList();
        for (Object po : poList) {
            Put put = convertPut(null,po);
            puts.add(put);
        }
        return puts;
    }


    /**
     * 传put将忽略rowkey注解
     * @param put
     * @param po
     * @return
     */
    private static Put convertPut(Put put,Object po)  {
        try{
            // convert value by the schema
            TableDefinition tableDefinition = AnnoSchemeUtils.findTableDefinition(po);
            List<ColumnDefinition> columnInfoList = tableDefinition.getColumnDefinitionList();
            RowkeyDefinition rowkeyDefinition = tableDefinition.getRowkeyDefinition();
            if(put==null){
                byte[] rowKey = resolveToBytes(rowkeyDefinition.getFieldType(), rowkeyDefinition.getRowKey().get(po));
                put = new Put(rowKey);
            }else {//put不为null，证明已经进入递归
                if(rowkeyDefinition.isHasRoweyAndColumnAnno()){//如果同时具有rowkey and column注解，进入递归rowkey需要解析为列族
                    if (rowkeyDefinition.getRowKey().get(po) == null) {

                    } else if (!rowkeyDefinition.hasColumnFamily()) {//如果列族没有注解配置，则表示这个是自定义对象，并且需要根据该对象的内部注解写hbase，同时要求必须是同一rowkey
                        put = convertPut(put,rowkeyDefinition.getRowKey().get(po));//递归
                    }
                    if(rowkeyDefinition.hasColumnFamily()){
                        byte[] fieldValue = resolveToBytes(rowkeyDefinition.getFieldType(), rowkeyDefinition.getRowKey().get(po));
                        put.addColumn(Bytes.toBytes(rowkeyDefinition.getColumnFamily()), Bytes.toBytes(rowkeyDefinition.getColumnName()), fieldValue);
                    }
                }
            }
            for (ColumnDefinition colDef : columnInfoList) {
                if (colDef.getField().get(po) == null) {
                    continue;
                } else if (!colDef.hasColumnFamily()) {//如果列族没有注解配置，则表示这个是自定义对象，并且需要根据该对象的内部注解写hbase，同时要求必须是同一rowkey
                    put = convertPut(put,colDef.getField().get(po));//递归
                }
                if(colDef.hasColumnFamily()){
                    byte[] fieldValue = resolveToBytes(colDef.getFieldType(), colDef.getField().get(po));
                    put.addColumn(Bytes.toBytes(colDef.getColumnFamily()), Bytes.toBytes(colDef.getColumnName()), fieldValue);
                }
            }
            return put;
        }catch (IllegalAccessException e){
            throw new HbaseClientException("[ERROR===>>>convert put error!]",e);
        }
    }

    private static byte[] resolveToBytes(Class<?> fieldType, Object fieldValue) throws IllegalAccessException {
        TypeResolver typeHandler = TypeResolverFactory.getResolver(fieldType);
        if (typeHandler == null) {
            throw new HbaseClientException("[ERROR===>>>]unsupport field type:" + fieldType.getTypeName());
        }
        return typeHandler.toBytes(fieldValue);
    }




    public static Delete wrapDelete(byte[] rowKey) {
        Preconditions.checkNotNull(rowKey, "[ERROR===>>>]rowKey can't be null");
        return new Delete(rowKey);
    }

    public static <T> T wrapResult(Class<T> type, Result result) throws InstantiationException, IllegalAccessException {
        return convertResult(type,result,false);
    }

    public static <T> List<T> wrapResultList(Class<T> type, Result[] results) throws IllegalAccessException, InstantiationException {
            List<T> resultList = Lists.newArrayList();
            for (Result result : results) {
                T target = convertResult(type,result,false);
                resultList.add(target);
            }
            return resultList;
    }

    //
    public static <T> T convertResult(Class<T> type, Result result,boolean isInRecursion) throws InstantiationException, IllegalAccessException {
        TableDefinition tableDefinition = AnnoSchemeUtils.findTableDefinition(type);
        List<ColumnDefinition> columnInfoList = tableDefinition.getColumnDefinitionList();
        RowkeyDefinition rowkeyDefinition = tableDefinition.getRowkeyDefinition();
        T target = null;
        Cell[] cells = result.rawCells();
        if (cells != null && cells.length > 0) {
            target = type.newInstance();
            // set rowkey value
            if (isInRecursion) {//进入递归，忽略rowkey注解
                if(rowkeyDefinition.isHasRoweyAndColumnAnno()){//具有column注解，当做普通字段处理
                    if (rowkeyDefinition.hasColumnFamily()) {
                        Cell cell = result.getColumnLatestCell(
                                Bytes.toBytes(rowkeyDefinition.getColumnFamily()),
                                Bytes.toBytes(rowkeyDefinition.getColumnName()));
                        if (cell != null) {
                            TypeResolver<?> typeResolver = TypeResolverFactory.getResolver(rowkeyDefinition.getFieldType());
                            rowkeyDefinition.getRowKey().set(target, typeResolver.toObject(CellUtil.cloneValue(cell)));
                        }
                    } else {
                        rowkeyDefinition.getRowKey().set(target, convertResult(rowkeyDefinition.getFieldType(), result, true));
                    }
                }
            } else {
                TypeResolver<?> rowKeyResolver = TypeResolverFactory.getResolver(rowkeyDefinition.getFieldType());
                rowkeyDefinition.getRowKey().set(target, rowKeyResolver.toObject(result.getRow()));
            }
            for (ColumnDefinition colDef : columnInfoList) {
                if (colDef.hasColumnFamily()) {
                    Cell cell = result.getColumnLatestCell(
                            Bytes.toBytes(colDef.getColumnFamily()),
                            Bytes.toBytes(colDef.getColumnName()));
                    if (cell == null) {
                        continue;
                    }
                    TypeResolver<?> typeResolver = TypeResolverFactory.getResolver(colDef.getFieldType());
                    colDef.getField().set(target, typeResolver.toObject(CellUtil.cloneValue(cell)));
                } else {
                    colDef.getField().set(target, convertResult(colDef.getFieldType(), result, true));
                }
            }
        }
        return target;
    }

    public static String findTableName(Class<?> po) {
        Preconditions.checkNotNull(po, "[ERROR===>>>]persistent object can't be null");
        Preconditions.checkState(po.isAnnotationPresent(com.maxplus1.hd_client.hbase.annotation.Table.class),
                        "[ERROR===>>>]persistent object must have the HBaseTable annotation");
        Table hbaseTable = po.getAnnotation(Table.class);
        String tableName = hbaseTable.name();
        Preconditions.checkState(StringUtils.isNotBlank(tableName), "[ERROR===>>>]tableName can't be empty");
        return tableName;
    }

    public static byte[] getRowKeyFromPo(Object po) throws IllegalAccessException {
        TableDefinition tableDefinition = AnnoSchemeUtils.findTableDefinition(po.getClass());
        RowkeyDefinition rowkeyDefinition = tableDefinition.getRowkeyDefinition();
        Object object = rowkeyDefinition.getRowKey().get(po);
        return resolveToBytes(rowkeyDefinition.getFieldType(), object);
    }

}
