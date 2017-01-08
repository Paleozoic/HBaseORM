package com.maxplus1.hd_client.hbase.operations.client.rtn_map;

import com.google.common.base.Preconditions;
import com.maxplus1.hd_client.hbase.cache.TableColumnCacheManager;
import com.maxplus1.hd_client.hbase.config.HBaseSource;
import com.maxplus1.hd_client.hbase.exception.HbaseClientException;
import com.maxplus1.hd_client.hbase.funciton.rtn_map.MapPersistent;
import com.maxplus1.hd_client.hbase.operations.beans.Column;
import com.maxplus1.hd_client.hbase.operations.beans.PageInfo;
import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.beans.ColumnTypeMetaData;
import com.maxplus1.hd_client.hbase.type.rtn_row.RowResolver;
import com.maxplus1.hd_client.hbase.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by qxloo on 2016/11/26.
 */
@Slf4j
@Deprecated
@Component("com.maxplus1.hd_client.hbase.operations.client.rtn_map.MapClient")
public class MapClient implements MapPersistent {

    @Resource
    private HBaseSource hBaseSource;

    @Override
    public List<Map> findList(String tableName, Scan scan, Filter... filters) {
        scan = ScanUtils.addFilters(scan, filters);
        Table table = null;
        try {
            table = this.hBaseSource.getTable(tableName);
        } catch (IOException e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
        List<Map> rowResults = ScanUtils.scanRows(table, scan);
        return rowResults;
    }

    @Override
    public Map get(String tableName, Get get, Filter... filters) {
        Table table = null;
        try {
            table = this.hBaseSource.getTable(tableName);
        } catch (IOException e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
        return GetUtils.get(table, get, filters);
    }

    @Override
    public List<Map> get(String tableName, List<Get> getList, Filter... filters) {
        Table table = null;
        try {
            table = this.hBaseSource.getTable(tableName);
            if (filters != null && filters.length > 0) {
                for (int i = 0; i < getList.size(); i++) {
                    getList.set(i, GetUtils.addFilters(getList.get(i), filters));
                }
            }
        } catch (IOException e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
        return GetUtils.getList(table, getList);
    }

    @Override
    public Map find(String tableName, String rowKey) {
        return find(tableName, null, rowKey);
    }

    @Override
    public Map find(String tableName, List<Column> columnList, String rowKey) {
        Get get = new Get(Bytes.toBytes(rowKey));
        if (columnList != null && !columnList.isEmpty()) {
            get = GetUtils.addColumns(get, columnList);
        }
        return get(tableName, get, null);
    }

    @Override
    public List<Map> findList(String tableName, List<String> rowKeyList, Filter... filters) {
        List<Get> getList = new ArrayList<>();
        if (filters != null) {
            for (String rowkey : rowKeyList) {
                Get get = new Get(Bytes.toBytes(rowkey));
                get = GetUtils.addFilters(get, filters);
                getList.add(get);
            }
        } else {
            for (String rowkey : rowKeyList) {
                getList.add(new Get(Bytes.toBytes(rowkey)));
            }
        }
        return get(tableName, getList);
    }

    @Override
    public List<Map> findList(String tableName, List<Column> columnList, List<String> rowKeyList, Filter... filters) {
        List<Get> getList = new ArrayList<>();
        if (filters != null) {
            for (String rowkey : rowKeyList) {
                Get get = new Get(Bytes.toBytes(rowkey));
                get = GetUtils.addFilters(get, filters);
                if (columnList != null && !columnList.isEmpty()) {
                    get = GetUtils.addColumns(get, columnList);
                }
                getList.add(get);
            }
        } else {
            for (String rowkey : rowKeyList) {
                Get get = new Get(Bytes.toBytes(rowkey));
                if (columnList != null && !columnList.isEmpty()) {
                    get = GetUtils.addColumns(get, columnList);
                }
                getList.add(get);
            }
        }
        return get(tableName, getList);
    }

    @Override
    public List<Map> findList(String tableName, String startRow, String endRow, Filter... filters) {
        return findList(tableName, null, startRow, endRow, filters);
    }

    @Override
    public List<Map> findList(String tableName, List<Column> columnList, String startRow, String endRow, Filter... filters) {
        Table table = null;
        try {
            table = this.hBaseSource.getTable(tableName);
        } catch (IOException e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
        Scan scan = ScanUtils.initScan(Bytes.toBytes(startRow), Bytes.toBytes(endRow), filters);
        if (columnList != null && !columnList.isEmpty()) {
            scan = ScanUtils.addColumns(scan, columnList);
        }
        return ScanUtils.scanRows(table, scan);
    }

    @Override
    public List<Map> findList(String tableName, String preRowkey, Filter... filters) {
        return findList(tableName, (List) null, preRowkey, filters);
    }

    @Override
    public List<Map> findList(String tableName, List<Column> columnList, String preRowkey, Filter... filters) {
        Table table = null;
        try {
            table = this.hBaseSource.getTable(tableName);
        } catch (IOException e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
        Scan scan = ScanUtils.initPrefixScan(Bytes.toBytes(preRowkey), filters);
        if (columnList != null && !columnList.isEmpty()) {
            scan = ScanUtils.addColumns(scan, columnList);
        }
        return ScanUtils.scanRows(table, scan);
    }

    @Override
    public void delete(String tableName, String rowKey) {
        Table table = null;
        try {
            table = this.hBaseSource.getTable(tableName);
        } catch (IOException e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
        DelUtils.delete(table, rowKey);
    }

    @Override
    public void deleteList(String tableName, List<String> rowKeyList) {
        Table table = null;
        try {
            table = this.hBaseSource.getTable(tableName);
        } catch (IOException e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
        DelUtils.deleteList(table, rowKeyList);
    }

    @Override
    public void put(String tableName, Map map) {
        Table table = null;
        try {
            table = this.hBaseSource.getTable(tableName);
        } catch (IOException e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }

        final Put put = new Put(Bytes.toBytes(map.get(Const.ROWKEY) + ""));
        map.forEach((k, v) -> {
            buildPut(tableName, put, k + "", v);
        });
        PutUtils.put(table, put);
    }

    @Override
    public void putList(String tableName, List<Map> mapList) {
        Table table = null;
        try {
            table = this.hBaseSource.getTable(tableName);
        } catch (IOException e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }

        List<Put> putList = new ArrayList<>();

        mapList.forEach(map -> {
            Put put = new Put(Bytes.toBytes(map.get(Const.ROWKEY) + ""));
            map.forEach((k, v) -> {
                buildPut(tableName, put, k + "", v);
            });
            putList.add(put);
        });
        PutUtils.put(table, putList);
    }

    @Override
    public PageInfo<Map> findListByPage(String tableName, PageInfo<Map> pageInfo, Filter... filters) {
        return findListByPage(tableName, null, pageInfo, filters);
    }

    @Override
    public PageInfo<Map> findListByPage(String tableName, List<Column> columnList, PageInfo<Map> pageInfo, Filter... filters) {
        Preconditions.checkArgument(pageInfo.getPageSize() > 0, "[ERROR===>>>]page size must greater than 0");
        Scan scan = ScanUtils.initScan(pageInfo, filters);
        if (columnList != null && !columnList.isEmpty()) {
            scan = ScanUtils.addColumns(scan, columnList);
        }
        List<Map> resultList;
        try (Table table = this.hBaseSource.getTable(tableName)) {
            resultList = ScanUtils.scanRows(table, scan);
            if (pageInfo.isPreviousPage()) {//如果是查询上一页数据，结果逆序
                Collections.reverse(resultList);
            }
            pageInfo.setDataSet(resultList);
            if (resultList != null && resultList.size() > 0) {
                // 取到最后一条数据的rowkey作为新的startRow
                String lastRowkey = (String) resultList.get(resultList.size() - 1).get(Const.ROWKEY);
                pageInfo = PageUtils.changePageInfo(pageInfo, Bytes.toBytes((String) resultList.get(0).get(Const.ROWKEY)), Bytes.toBytes(lastRowkey));
            }
        } catch (Exception e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
        return pageInfo;
    }

    private Put buildPut(String tableName, Put put, String k, Object v) {
        if (!Const.ROWKEY.equals(k)) {
            String[] cf_col = k.split(Const.CF_COL_SPT);
            String familyName = cf_col[0];
            String columnName = cf_col[1];
            ColumnTypeMetaData.ColumnType columnType = TableColumnCacheManager.get(tableName, familyName, columnName);
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), RowResolver.toBytes(columnType, v));
        }
        return put;
    }
}
