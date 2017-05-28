package com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans;

import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.beans.ColumnMetaData;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.lang.reflect.Field;

/**
 * wrap column definition for schema
 * 
 * @author zachary.zhang
 * @author edit by Paleo
 */
@Data
public class ColumnDefinition extends ColumnMetaData{

    private Field field;
    private Class<?> fieldType;

}
