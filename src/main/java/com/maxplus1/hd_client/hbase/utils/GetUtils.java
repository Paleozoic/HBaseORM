package com.maxplus1.hd_client.hbase.utils;

import com.google.common.collect.Lists;
import com.maxplus1.hd_client.hbase.cache.TableDefinitionCacheManager;
import com.maxplus1.hd_client.hbase.operations.beans.Column;
import com.maxplus1.hd_client.hbase.type.rtn_row.RowResolver;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Created by qxloo on 2016/11/30.
 */
@Slf4j
public enum GetUtils {
    ;

    public static Get initGet(String rowkey, Class classType) {
        Get get = new Get(Bytes.toBytes(rowkey));
        return TableDefinitionCacheManager.buildGet(get,classType,false);
    }

    public static Get initGet(byte[] rowkey, Class classType) {
        Get get = new Get(rowkey);
        return TableDefinitionCacheManager.buildGet(get,classType,false);
    }

    public static Map get(Table table, Get get, Filter ... filters) {
        try {
            get = GetUtils.addFilters(get,filters);
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            if(cells==null||cells.length==0){
                return null;
            }
            String tableName = table.getName().getNameAsString();
            Map rowResult = new HashMap();
            for (Cell cell : cells) {
                rowResult.put(Const.ROWKEY,Bytes.toString(CellUtil.cloneRow(cell)));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                rowResult.put(family+Const.CF_COL_SPT+column,RowResolver.buildCellResult(tableName,family,column,cell));
            }
            return rowResult;
        } catch (IOException e) {
            log.error("[ERROR===>>>]", e);
        }
        return null;
    }

    public static List<Map> getList(Table table, List<Get> getList) {
        try {
            List<Map> list = new ArrayList<>();
            Result[] resultList = table.get(getList);
            String tableName = table.getName().getNameAsString();
            for (Result result : resultList) {
                Map rowResult = new HashMap();
                for (Cell cell : result.rawCells()) {
                    rowResult.put(Const.ROWKEY,Bytes.toString(CellUtil.cloneRow(cell)));
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                    rowResult.put(family+Const.CF_COL_SPT+column,RowResolver.buildCellResult(tableName,family,column,cell));
                }
                list.add(rowResult);
            }
            return list;
        } catch (IOException e) {
            log.error("[ERROR===>>>]", e);
        }
        return null;
    }

    public static Get addFilters(Get get, Filter[] filters) {
        List<Filter> filterList = Lists.newArrayList();
        Filter filter = get.getFilter();
        if(filter!=null){
            /**
             * 如果FilterList多了一个null，会一直报这个错：
             2016-12-04 17:05:53,373:INFO hconnection-0x339d1500-shared--pool1-t1 (RpcRetryingCaller.java:142) - Call exception, tries=10, retries=35, started=38364 ms ago, cancelled=false, msg=row '20130701' on table 'aliyun:mfd_bank_shibor' at region=aliyun:mfd_bank_shibor,,1480469896789.c11dbc06b0070b77820a2fedff484597., hostname=hdnode,16020,1480428580174, seqNum=2
             */
            filterList.add(filter);
        }
        if (!FilterUtils.isAllNull(filters)) {
            filterList.addAll(Arrays.asList(filters));
        }
        if (filterList.size() > 0) {
            FilterList f = new FilterList(filterList.toArray(new Filter[filterList.size()]));
            get.setFilter(f);
        }
        return get;
    }

    @Deprecated
    public static Get addColumns(Get get, List<Column> columnList) {
        columnList.forEach(column -> {
            if (column.getColumnName() != null && column.getColumnName().length() > 0) {
                get.addColumn(Bytes.toBytes(column.getFamilyName()), Bytes.toBytes(column.getColumnName()));
            } else {
                get.addFamily(Bytes.toBytes(column.getFamilyName()));
            }
        });
        return get;
    }

}
