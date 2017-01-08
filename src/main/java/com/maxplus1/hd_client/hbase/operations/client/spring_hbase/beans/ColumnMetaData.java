package com.maxplus1.hd_client.hbase.operations.client.spring_hbase.beans;


/**
 * wrap column definition for schema
 */
public class ColumnMetaData {
    private String tableName;
    private String columnFamily;
    private String columnName;

    public ColumnMetaData() {
    }
    public ColumnMetaData(String tableName, String columnFamily, String columnName) {
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.columnName = columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnMetaData)) return false;

        ColumnMetaData that = (ColumnMetaData) o;

        if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null) return false;
        if (columnFamily != null ? !columnFamily.equals(that.columnFamily) : that.columnFamily != null) return false;
        if (columnName != null ? !columnName.equals(that.columnName) : that.columnName != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tableName != null ? tableName.hashCode() : 0;
        result = 31 * result + (columnFamily != null ? columnFamily.hashCode() : 0);
        result = 31 * result + (columnName != null ? columnName.hashCode() : 0);
        return result;
    }

    public boolean hasColumnFamily(){
        return getColumnFamily()!=null&&getColumnFamily().length()>0;
    }
}
