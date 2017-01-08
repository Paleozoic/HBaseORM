package com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans;

import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.beans.ColumnMetaData;
import lombok.Getter;
import lombok.Setter;

import java.lang.reflect.Field;

@Getter
@Setter
public class RowkeyDefinition extends ColumnMetaData {

    private Field rowKey;
    private Class<?> fieldType;
    private boolean hasRoweyAndColumnAnno = false;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RowkeyDefinition)) return false;
        if (!super.equals(o)) return false;

        RowkeyDefinition that = (RowkeyDefinition) o;

        if (hasRoweyAndColumnAnno != that.hasRoweyAndColumnAnno) return false;
        if (rowKey != null ? !rowKey.equals(that.rowKey) : that.rowKey != null) return false;
        return fieldType != null ? fieldType.equals(that.fieldType) : that.fieldType == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (rowKey != null ? rowKey.hashCode() : 0);
        result = 31 * result + (fieldType != null ? fieldType.hashCode() : 0);
        result = 31 * result + (hasRoweyAndColumnAnno ? 1 : 0);
        return result;
    }
}
