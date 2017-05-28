package com.maxplus1.hd_client.hbase.operations.client.rtn_pojo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.maxplus1.hd_client.hbase.config.HBaseSource;
import com.maxplus1.hd_client.hbase.funciton.BytesReadable;
import com.maxplus1.hd_client.hbase.funciton.BytesWriteable;
import com.maxplus1.hd_client.hbase.operations.PageInfo;
import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.TableCallback;
import com.maxplus1.hd_client.hbase.utils.*;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * 底层接口
 * @author zachary.zhang
 * @author edit by Paleo
 */
@Slf4j
@NoArgsConstructor
@Component("com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.BytesClient")
public class BytesClient implements BytesReadable,BytesWriteable {

    @Resource
    private HBaseSource hBaseSource;

    @Override
    public <T> T find(byte[] rowKey, Class<T> type)  {
        Preconditions.checkNotNull(rowKey, "[ERROR===>>>]rowKey can't be null");
        Table table =  this.hBaseSource.getTable(HBaseUtils.findTableName(type));
        return TableTemplate.opt(table,new TableCallback<T>() {
            @Override
            public T doInTable(Table table) throws IOException, IllegalAccessException, InstantiationException {
                Get get = GetUtils.initGet(rowKey,type);
                Result result = table.get(get);
                return HBaseUtils.wrapResult(type, result);
            }
        });
    }


    @Override
    public <T> List<T> findList(List<byte[]> rowKeyList, Class<T> type){
        Preconditions.checkNotNull(rowKeyList, "[ERROR===>>>]rowKey list can't be null");
        Preconditions.checkArgument(rowKeyList.size() != 0, "[ERROR===>>>]rowKey  list can't be empty");
        Table table =  this.hBaseSource.getTable(HBaseUtils.findTableName(type));
        return TableTemplate.opt(table,new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(Table table) throws IOException, IllegalAccessException, InstantiationException {
                List<Get> gets = Lists.newArrayList();
                for (byte[] rowKey : rowKeyList) {
                    Get get = GetUtils.initGet(rowKey,type);
                    gets.add(get);
                }

                Result[] results = table.get(gets);

                return HBaseUtils.wrapResultList(type, results);
            }
        });
    }

    @Override
    public <T> List<T> findList(byte[] startRow, byte[] endRow, Class<T> type) {
        Preconditions.checkNotNull(type, "[ERROR===>>>]class type can't be null");
        Table table =  this.hBaseSource.getTable(HBaseUtils.findTableName(type));
        return TableTemplate.opt(table, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(Table table) throws IOException, IllegalAccessException, InstantiationException {
                Scan scan = ScanUtils.initScan(startRow, endRow);

                List<T> resultList = Lists.newArrayList();
                ResultScanner scanner = table.getScanner(scan);

                Result result = null;
                while ((result = scanner.next()) != null) {
                    T wrapResult = HBaseUtils.wrapResult(type, result);
                    resultList.add(wrapResult);
                }
                return resultList;
            }
        });
    }

    @Override
    public <T> List<T> findList(byte[] preRowkey, Class<T> type)  {
        Preconditions.checkNotNull(type, "[ERROR===>>>]class type can't be null");
        Table table =  this.hBaseSource.getTable(HBaseUtils.findTableName(type));
        return TableTemplate.opt(table, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(Table table) throws IOException, IllegalAccessException, InstantiationException {
                byte[] startRow = null,endRow = null;
                Scan scan = ScanUtils.initScan(startRow, endRow,new PrefixFilter(preRowkey));

                List<T> resultList = Lists.newArrayList();
                ResultScanner scanner = table.getScanner(scan);

                Result result = null;
                while ((result = scanner.next()) != null) {
                    T wrapResult = HBaseUtils.wrapResult(type, result);
                    resultList.add(wrapResult);
                }

                return resultList;
            }
        });
    }


    @Override
    public void put(Object po)  {
        Preconditions.checkNotNull(po, "[ERROR===>>>]persistent object can't be null");
        Table table = hBaseSource.getTable(HBaseUtils.findTableName(po.getClass()));
        TableTemplate.opt(table, new TableCallback() {
            @Override
            public Object doInTable(Table table) throws IOException, IllegalAccessException, InstantiationException {
                Put put = HBaseUtils.wrapPut(po);
                table.put(put);
                return null;
            }
        });

    }

    @Override
    public <T> void putList(List<T> poList)  {
        Preconditions.checkNotNull(poList, "[ERROR===>>>]persistent object list can't be null");
        Preconditions.checkArgument(poList.size() != 0, "[ERROR===>>>]persistent object list can't be empty");
        Table table = hBaseSource.getTable(HBaseUtils.findTableName(poList.get(0).getClass()));
        TableTemplate.opt(table, new TableCallback() {
            @Override
            public Object doInTable(Table table) throws IOException, IllegalAccessException, InstantiationException {
                List<Put> puts = HBaseUtils.wrapPutList(poList);
                table.put(puts);
                return null;
            }
        });
    }

    @Override
    public void delete(byte[] rowKey, Class<?> po)  {
        Preconditions.checkNotNull(po, "[ERROR===>>>]persistent po class can't be null");
        Table table = hBaseSource.getTable(HBaseUtils.findTableName(po));
        TableTemplate.opt(table, new TableCallback() {
            @Override
            public Object doInTable(Table table) throws Throwable {
                table.delete(HBaseUtils.wrapDelete(rowKey));
                return null;
            }
        });
    }


    @Override
    public void deleteList(List<byte[]> rowKeyList, Class<?> po) {
        Preconditions.checkNotNull(po, "[ERROR===>>>]persistent po class can't be null");
        Table table = hBaseSource.getTable(HBaseUtils.findTableName(po));
        TableTemplate.opt(table, new TableCallback() {
            @Override
            public Object doInTable(Table table) throws IOException, IllegalAccessException, InstantiationException {
                List<Delete> deleteList = Lists.newArrayList();
                for (byte[] rowKey : rowKeyList) {
                    deleteList.add(HBaseUtils.wrapDelete(rowKey));
                }
                table.delete(deleteList);
                return null;
            }
        });
    }

    @Override
    public <T> PageInfo<T> findListByPage(final PageInfo<T> pageInfo, Filter... filters) {

        Preconditions.checkArgument(pageInfo.getPageSize() > 0, "[ERROR===>>>]page size must greater than 0");

        Scan scan = ScanUtils.initScan(pageInfo,pageInfo.getPoClass(),filters);

        List<T> resultList = Lists.newArrayList();
        Table table = hBaseSource.getTable(HBaseUtils.findTableName(pageInfo.getPoClass()));
        return TableTemplate.opt(table, new TableCallback<PageInfo<T>>() {
            @Override
            public PageInfo<T> doInTable(Table table) throws IOException, IllegalAccessException, InstantiationException {
                ResultScanner scanner = table.getScanner(scan);
                Result result = null;
                while ((result = scanner.next()) != null) {
                    T wrapResult = HBaseUtils.wrapResult(pageInfo.getPoClass(), result);
                    resultList.add(wrapResult);
                }
                if(pageInfo.isPreviousPage()){//如果是查询上一页数据，结果逆序
                    Collections.reverse(resultList);
                }
                pageInfo.setDataSet(resultList);
                if (resultList != null && resultList.size() > 0) {
                    // 取到最后一条数据的rowkey作为新的startRow
                    T t = resultList.get(resultList.size() - 1);
                    //改变pageInfo的一些数据
                    PageUtils.changePageInfo(pageInfo, HBaseUtils.getRowKeyFromPo(resultList.get(0)),HBaseUtils.getRowKeyFromPo(t));
                }
                return pageInfo;
            }
        });
    }

}
