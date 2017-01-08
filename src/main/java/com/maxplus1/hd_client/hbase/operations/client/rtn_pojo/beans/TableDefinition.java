package com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.beans;

import lombok.Data;

import java.util.List;

/**
 * Created by qxloo on 2016/12/22.
 */
@Data
public class TableDefinition {
    private RowkeyDefinition rowkeyDefinition;
    private List<ColumnDefinition> columnDefinitionList;
}
