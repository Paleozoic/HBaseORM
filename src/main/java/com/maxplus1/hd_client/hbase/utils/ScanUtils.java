package com.maxplus1.hd_client.hbase.utils;

import com.google.common.collect.Lists;
import com.maxplus1.hd_client.hbase.cache.TableDefinitionCacheManager;
import com.maxplus1.hd_client.hbase.operations.beans.Column;
import com.maxplus1.hd_client.hbase.operations.beans.PageInfo;
import com.maxplus1.hd_client.hbase.type.rtn_row.RowResolver;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;


@Slf4j
public enum ScanUtils {
    ;

    private static final int PAGE_SIZE_NO_LIMIT = -1;
    private static int scanCaching = 100;// default cache in scan
    // TODO use it in future
    @SuppressWarnings("unused")
    private static int scanBatch = 100;// default batch in scan

    public static Scan addFilters(Scan scan, Filter... filters) {
        List<Filter> filterList = Lists.newArrayList();
        Filter filter = scan.getFilter();
        if (filter != null) {
            filterList.add(filter);
        }
        if (!FilterUtils.isAllNull(filters)) {
            filterList.addAll(Arrays.asList(filters));
        }
        if (filterList.size() > 0) {
            FilterList f = new FilterList(filterList.toArray(new Filter[filterList.size()]));
            scan.setFilter(f);
        }
        return scan;
    }

    public static Scan initScan(PageInfo pageInfo, Class classType, Filter... filters) {
        Scan scan = new Scan();
        if (classType != null) {
            scan = TableDefinitionCacheManager.buildScan(scan, classType, false);
        }
        Filter filter = new PageFilter(pageInfo.getPageSize());
        scan.setFilter(filter);
        scan = ScanUtils.addFilters(scan, filters);
        if (pageInfo.isPreviousPage()) {//向上翻页，不设置stopRow
            scan.setReversed(true);
            scan.setStartRow(pageInfo.getLastStartRow());
        } else {
            scan.setStartRow(pageInfo.getStartRow());
        }
        return scan;
    }

    /*public static Scan initScan(byte[] startRow, byte[] endRow, Class classType,Filter... filters) {
        return initScan(startRow, endRow, PAGE_SIZE_NO_LIMIT, classType,filters);
    }*/

    public static Scan initPrefixScan(byte[] prefix, Class classType, Filter... filters) {
        Scan scan = new Scan();
        if (classType != null) {
            scan = TableDefinitionCacheManager.buildScan(scan, classType, false);
        }
        Filter filter = new PrefixFilter(prefix);
        scan.setFilter(filter);
        scan = addFilters(scan, filters);
        scan.setCaching(scanCaching);
        // scan.setBatch(scanBatch);
        return scan;
    }

    /**
     * @param startRow
     * @param endRow
     * @param pageSize
     * @param filters
     * @return
     */
    public static Scan initScan(byte[] startRow, byte[] endRow, int pageSize, Class classType, Filter... filters) {
        Scan scan = new Scan();
        if (classType != null) {
            scan = TableDefinitionCacheManager.buildScan(scan, classType, false);
        }
        //scan 默认值是startRow=stopRow=HConstants.EMPTY_START_ROW
        if (startRow != null) {
            scan.setStartRow(startRow);
        }

        if (endRow != null) {
            scan.setStopRow(endRow);
        }

        if (pageSize != PAGE_SIZE_NO_LIMIT) {
            Filter filter = new PageFilter(pageSize);
            scan.setFilter(filter);
        }

        scan = addFilters(scan, filters);

        scan.setCaching(scanCaching);
        // scan.setBatch(scanBatch);
        return scan;
    }

    public static Scan initScan(byte[] startRow, byte[] endRow, Filter... filters) {
        return initScan(startRow, endRow, PAGE_SIZE_NO_LIMIT, null, filters);
    }

    public static Scan initScan(PageInfo<Map> pageInfo, Filter[] filters) {
        return initScan(pageInfo, null, filters);
    }

    public static Scan initPrefixScan(byte[] prefix, Filter... filters) {
        return initPrefixScan(prefix, null, filters);
    }

    //==============MapClient start==========
    @Deprecated
    public static Scan addColumns(Scan scan, List<Column> columnList) {
        columnList.forEach(column -> {
            if (column.getColumnName() != null && column.getColumnName().length() > 0) {
                scan.addColumn(Bytes.toBytes(column.getFamilyName()), Bytes.toBytes(column.getColumnName()));
            } else {
                scan.addFamily(Bytes.toBytes(column.getFamilyName()));
            }
        });
        return scan;
    }

    @Deprecated
    public static List<Map> scanRows(Table table, Scan scan) {
        ResultScanner resultScanner = null;
        try {
            String tableName = table.getName().getNameAsString();
            List res = new ArrayList<Map>();
            resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                Map rowResult = new HashMap();
                for (Cell cell : result.rawCells()) {
                    rowResult.put(Const.ROWKEY, Bytes.toString(CellUtil.cloneRow(cell)));
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                    rowResult.put(family + Const.CF_COL_SPT + column, RowResolver.buildCellResult(tableName, family, column, cell));
                }
                res.add(rowResult);
            }
            return res;
        } catch (IOException e) {
            log.error("[ERROR===>>>]scan rows error!", e);
            return null;
        } finally {
            try {
                if (resultScanner != null) {
                    resultScanner.close();
                }
                if (table != null) {
                    table.close();
                }
            } catch (IOException e) {
                log.error("[ERROR===>>>]scanner or table close error!", e);
            }
        }
    }

    //==============MapClient end==========

}
