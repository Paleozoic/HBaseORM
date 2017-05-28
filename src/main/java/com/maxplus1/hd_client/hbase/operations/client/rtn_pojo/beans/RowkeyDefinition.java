package com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans;

import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.beans.ColumnMetaData;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.lang.reflect.Field;

@Data
public class RowkeyDefinition extends ColumnMetaData {

    private Field rowKey;
    private Class<?> fieldType;
    private boolean hasRoweyAndColumnAnno = false;

}
