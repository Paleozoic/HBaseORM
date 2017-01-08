package com.maxplus1.hd_client.hbase.operations.client.spring_hbase.beans;


import javax.activation.UnsupportedDataTypeException;

/**
 * wrap column definition for schema
 */
public class ColumnTypeMetaData extends ColumnMetaData{

    private ColumnType columnType;

    public ColumnTypeMetaData() {
    }


    public enum ColumnType {
        STRING("STRING"),
        BIGDECIMAL("BIGDECIMAL"),
        BOOLEAN("BOOLEAN"),
        BYTE("BYTE"),
        SHORT("SHORT"),
        INTEGER("INTEGER"),
        LONG("LONG"),
        FLOAT("FLOAT"),
        DATE("DATE"),
        JAVA_SET("JAVA_SET"),
        MY_OBJ("MY_OBJ"),
        DOUBLE("DOUBLE");

        private String type;

        ColumnType(String type) {
            this.type = type;
        }

        public String getType(){
            return type;
        }
    }

    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }

    public void setColumnType(String columnType) throws UnsupportedDataTypeException {
        switch (columnType){
            case "STRING":{
                this.columnType = ColumnType.STRING;
                break;
            }
            case "BIGDECIMAL":{
                this.columnType = ColumnType.BIGDECIMAL;
                break;
            }
            case "BOOLEAN":{
                this.columnType = ColumnType.BOOLEAN;
                break;
            }
            case "BYTE":{
                this.columnType = ColumnType.BYTE;
                break;
            }
            case "SHORT":{
                this.columnType = ColumnType.SHORT;
                break;
            }
            case "INTEGER":{
                this.columnType = ColumnType.INTEGER;
                break;
            }
            case "LONG":{
                this.columnType = ColumnType.LONG;
                break;
            }
            case "FLOAT":{
                this.columnType = ColumnType.FLOAT;
                break;
            }
            case "DOUBLE":{
                this.columnType = ColumnType.DOUBLE;
                break;
            }
            case "DATE":{
                this.columnType = ColumnType.DATE;
                break;
            }
            case "JAVA_SET":{
                this.columnType = ColumnType.JAVA_SET;
                break;
            }
            case "MY_OBJ":{
                this.columnType = ColumnType.MY_OBJ;
                break;
            }
            default:{
                throw new UnsupportedDataTypeException("不支持的数据类型！");
            }
        }
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnTypeMetaData)) return false;
        if (!super.equals(o)) return false;

        ColumnTypeMetaData that = (ColumnTypeMetaData) o;

        return columnType == that.columnType;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (columnType != null ? columnType.hashCode() : 0);
        return result;
    }
}
