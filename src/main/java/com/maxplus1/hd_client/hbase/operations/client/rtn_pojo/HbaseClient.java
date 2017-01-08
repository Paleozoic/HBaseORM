package com.maxplus1.hd_client.hbase.operations.client.rtn_pojo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.maxplus1.hd_client.hbase.config.HBaseSource;
import com.maxplus1.hd_client.hbase.exception.HbaseClientException;
import com.maxplus1.hd_client.hbase.funciton.rtn_pojo.Persistent;
import com.maxplus1.hd_client.hbase.operations.beans.PageInfo;
import com.maxplus1.hd_client.hbase.utils.HBaseUtils;
import com.maxplus1.hd_client.hbase.utils.PageUtils;
import com.maxplus1.hd_client.hbase.utils.ScanUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author zachary.zhang
 * @author Paleo
 */
@Slf4j
@Component("com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.HbaseClient")
public class HbaseClient implements Persistent {

    @Resource
    private BytesClient bytesClient;

    @Resource
    private HBaseSource hBaseSource;

    @Override
    public <T> T find(String rowKey, Class<T> type)  {
        return bytesClient.find(Bytes.toBytes(rowKey),type);
    }

    @Override
    public <T> List<T> findList(List<String> rowKeyList, Class<T> type)  {
        List<byte[]> rks = new ArrayList<>();
        for(String rk:rowKeyList){
            rks.add(Bytes.toBytes(rk));
        }
        return bytesClient.findList(rks,type);
    }

    @Override
    public <T> List<T> findList(String startRow, String endRow, Class<T> type)  {
        return bytesClient.findList(Bytes.toBytes(startRow),Bytes.toBytes(endRow),type);
    }

    @Override
    public <T> List<T> findList(String preRowkey, Class<T> type)  {
        return bytesClient.findList(Bytes.toBytes(preRowkey),type);
    }

    @Override
    public void delete(String rowKey, Class<?> po)  {
        bytesClient.delete(Bytes.toBytes(rowKey),po);
    }

    @Override
    public void deleteList(List<String> rowKeyList, Class<?> po)  {
        List<byte[]> rks = new ArrayList<>();
        for(String rk:rowKeyList){
            rks.add(Bytes.toBytes(rk));
        }
        bytesClient.deleteList(rks,po);
    }

    @Override
    public void put(Object po)  {
        Preconditions.checkNotNull(po, "[ERROR===>>>]persistent object can't be null");

        // auto close the resources
        try (Table table = hBaseSource.getTable(HBaseUtils.findTableName(po.getClass()))) {
            Put put = HBaseUtils.wrapPut(po);
            table.put(put);
        } catch (Exception e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
    }

    @Override
    public <T> void putList(List<T> poList)  {
        Preconditions.checkNotNull(poList, "[ERROR===>>>]persistent object list can't be null");
        Preconditions.checkArgument(poList.size() != 0, "[ERROR===>>>]persistent object list can't be empty");
        try (Table table = hBaseSource.getTable(HBaseUtils.findTableName(poList.get(0).getClass()))) {
            List<Put> puts = HBaseUtils.wrapPutList(poList);
            table.put(puts);
        } catch (Exception e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
    }

    @Override
    public <T> PageInfo<T> findListByPage(PageInfo<T> pageInfo, Filter... filters) {

        Preconditions.checkArgument(pageInfo.getPageSize() > 0, "[ERROR===>>>]page size must greater than 0");

        Scan scan = ScanUtils.initScan(pageInfo,pageInfo.getPoClass(),filters);

        List<T> resultList = Lists.newArrayList();
        try (Table table = hBaseSource.getTable(HBaseUtils.findTableName(pageInfo.getPoClass()))) {
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
                pageInfo = PageUtils.changePageInfo(pageInfo, HBaseUtils.getRowKeyFromPo(resultList.get(0)),HBaseUtils.getRowKeyFromPo(t));
            }
        } catch (Exception e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
        return pageInfo;
    }

}
