package com.maxplus1.hd_client.hbase.utils;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.maxplus1.hd_client.hbase.annotation.Column;
import com.maxplus1.hd_client.hbase.annotation.RowKey;
import com.maxplus1.hd_client.hbase.cache.TableDefinitionCacheManager;
import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans.ColumnDefinition;
import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans.RowkeyDefinition;
import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans.TableDefinition;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by qxloo on 2017/1/4.
 */
public enum AnnoSchemeUtils {
    ;
    /**
     * 找到column info,返回list
     *
     * @param po 持久化对象
     * @return List<ColumnInfo>
     */
    public static TableDefinition findTableDefinition(Object po) {
        return findTableDefinition(po.getClass());
    }

    /**
     * 根据class type找到column info,返回list
     *
     * @param classType
     * @return
     */
    public static TableDefinition findTableDefinition(Class<?> classType) {

        TableDefinition tableDefinition = TableDefinitionCacheManager.get(classType);

        if(tableDefinition==null){
            tableDefinition = new TableDefinition();

            Field[] fields = classType.getDeclaredFields();
            List<ColumnDefinition> columnDefinitions = new ArrayList<>();
            for (Field field : fields) {
                field.setAccessible(true);
                if (field.isAnnotationPresent(RowKey.class)&&field.isAnnotationPresent(Column.class)) {
                    RowkeyDefinition rowkeyDefinition = new RowkeyDefinition();
                    rowkeyDefinition.setFieldType(field.getType());
                    rowkeyDefinition.setRowKey(field);
                    rowkeyDefinition.setHasRoweyAndColumnAnno(true);

                    Column hbaseColumn = field.getAnnotation(Column.class);
                    Preconditions.checkState(!(StringUtils.isBlank(hbaseColumn.family()) && SysUtils.isJavaClass(classType)), "[ERROR===>>>]if not customized type,operations column family can't be null");
                    rowkeyDefinition.setColumnFamily(hbaseColumn.family());
                    rowkeyDefinition.setFieldType(field.getType());
                    rowkeyDefinition.setColumnName(field.getName());
                    // 如果注解中没有定义columnName，则使用property name作为默认值
                    if (StringUtils.isBlank(hbaseColumn.name())) {
                        // 驼峰式命名转化成小写下划线命名，如：xxYy ---> xx_yy
                        rowkeyDefinition.setColumnName(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName()));
                    } else {
                        rowkeyDefinition.setColumnName(hbaseColumn.name());
                    }

                    tableDefinition.setRowkeyDefinition(rowkeyDefinition);
                } else if (field.isAnnotationPresent(RowKey.class)) {
                    RowkeyDefinition rowkeyDefinition = new RowkeyDefinition();
                    rowkeyDefinition.setFieldType(field.getType());
                    rowkeyDefinition.setRowKey(field);
                    tableDefinition.setRowkeyDefinition(rowkeyDefinition);
                } else if (field.isAnnotationPresent(Column.class)) {
                    Column hbaseColumn = field.getAnnotation(Column.class);
                    Preconditions.checkState(!(StringUtils.isBlank(hbaseColumn.family()) && SysUtils.isJavaClass(classType)), "[ERROR===>>>]if not customized type,operations column family can't be null");
                    ColumnDefinition colDef = new ColumnDefinition();
                    colDef.setColumnFamily(hbaseColumn.family());
                    colDef.setFieldType(field.getType());
                    colDef.setColumnName(field.getName());
                    colDef.setField(field);
                    // 如果注解中没有定义columnName，则使用property name作为默认值
                    if (StringUtils.isBlank(hbaseColumn.name())) {
                        // 驼峰式命名转化成小写下划线命名，如：xxYy ---> xx_yy
                        colDef.setColumnName(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName()));
                    } else {
                        colDef.setColumnName(hbaseColumn.name());
                    }
                    columnDefinitions.add(colDef);
                }else{
                    // 没有注解的列，不用做映射
                }
            }
            tableDefinition.setColumnDefinitionList(columnDefinitions);
            TableDefinitionCacheManager.put(classType,tableDefinition);
        }
        return tableDefinition;
    }
}
