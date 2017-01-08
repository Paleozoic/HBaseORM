package com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans;

import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.beans.ColumnMetaData;
import lombok.Getter;
import lombok.Setter;

import java.lang.reflect.Field;

/**
 * wrap column definition for schema
 * 
 * @author zachary.zhang
 * @author edit by Paleo
 */
@Getter
@Setter
public class ColumnDefinition extends ColumnMetaData{

    private Field field;
    private Class<?> fieldType;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnDefinition)) return false;
        if (!super.equals(o)) return false;

        ColumnDefinition that = (ColumnDefinition) o;

        if (field != null ? !field.equals(that.field) : that.field != null) return false;
        return fieldType != null ? fieldType.equals(that.fieldType) : that.fieldType == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (field != null ? field.hashCode() : 0);
        result = 31 * result + (fieldType != null ? fieldType.hashCode() : 0);
        return result;
    }
}
