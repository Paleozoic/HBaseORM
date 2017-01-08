package com.maxplus1.hd_client.hbase.operations.beans;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Deprecated
public class Column {
    private String familyName;
    private String columnName;

    public Column() {
    }

    public Column(String familyName) {
        this.familyName = familyName;
    }

    public Column(String familyName,String columnName) {
        this.familyName = familyName;
        this.columnName = columnName;
    }
}
