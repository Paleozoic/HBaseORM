package com.maxplus1.hd_client.hbase.utils;

import com.maxplus1.hd_client.hbase.cache.TableDefinitionCacheManager;
import com.maxplus1.hd_client.hbase.operations.PageInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.util.Map;


@Slf4j
public enum ScanUtils {
    ;

    private static final int PAGE_SIZE_NO_LIMIT = -1;
    /**
     * TODO setCaching和setBatch性能优化
     * scan.setCaching(scanCaching); 每次RPC返回的记录数
     * scan.setBatch(scanBatch); 每次RPC传ROW的几个Column
     * 所以cache和batch共同决定了RPC的次数
     */
    private static int scanCaching = 0;// default cache in scan
    private static int scanBatch = 0;// default batch in scan


    public static Scan initScan(PageInfo pageInfo, Class classType, Filter... filters) {
        Scan scan = new Scan();
        if (classType != null) {
            scan = TableDefinitionCacheManager.buildQuery(scan, classType, false);
        }
        Filter filter = new PageFilter(pageInfo.getPageSize());
        scan.setFilter(filter);
        scan = FilterUtils.addFilters(scan, filters);

        if (pageInfo.isPreviousPage()) {//向上翻页，不设置stopRow
            scan.setReversed(true);
            scan.setStartRow(pageInfo.getLastStartRow());
        } else {
            scan.setStartRow(pageInfo.getStartRow());
            if(pageInfo.getStopRow()!=null){
                scan.setStopRow(pageInfo.getStopRow());
            }
        }
        return scan;
    }

    /*public static Scan initScan(byte[] startRow, byte[] endRow, Class classType,Filter... filters) {
        return initScan(startRow, endRow, PAGE_SIZE_NO_LIMIT, classType,filters);
    }*/

    public static Scan initPrefixScan(byte[] prefix, Class classType, Filter... filters) {
        Scan scan = new Scan();
        if (classType != null) {
            scan = TableDefinitionCacheManager.buildQuery(scan, classType, false);
        }
        Filter filter = new PrefixFilter(prefix);
        scan.setFilter(filter);
        scan = FilterUtils.addFilters(scan, filters);
//        scan.setCaching(scanCaching);
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
            scan = TableDefinitionCacheManager.buildQuery(scan, classType, false);
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

        scan = FilterUtils.addFilters(scan, filters);

//        scan.setCaching(scanCaching);
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

}
