package com.maxplus1.hd_client.hbase.operations.client.rtn_pojo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.maxplus1.hd_client.hbase.config.HBaseSource;
import com.maxplus1.hd_client.hbase.exception.HbaseClientException;
import com.maxplus1.hd_client.hbase.funciton.rtn_pojo.BytesPersistent;
import com.maxplus1.hd_client.hbase.utils.GetUtils;
import com.maxplus1.hd_client.hbase.utils.HBaseUtils;
import com.maxplus1.hd_client.hbase.utils.ScanUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author zachary.zhang
 * @author edit by Paleo
 */
@Slf4j
@Component("com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.BytesClient")
public class BytesClient implements BytesPersistent {

    @Resource
    private HBaseSource hBaseSource;


    public BytesClient() {
    }


    @Override
    public <T> T find(byte[] rowKey, Class<T> type)  {
        Preconditions.checkNotNull(rowKey, "[ERROR===>>>]rowKey can't be null");
        try (Table table = this.hBaseSource.getTable(HBaseUtils.findTableName(type))) {
            Get get = GetUtils.initGet(rowKey,type);
            Result result = table.get(get);
            return HBaseUtils.wrapResult(type, result);
        } catch (Exception e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
    }


    @Override
    public <T> List<T> findList(List<byte[]> rowKeyList, Class<T> type){
        Preconditions.checkNotNull(rowKeyList, "[ERROR===>>>]rowKey list can't be null");
        Preconditions.checkArgument(rowKeyList.size() != 0, "[ERROR===>>>]rowKey  list can't be empty");

        try (Table table = this.hBaseSource.getTable(HBaseUtils.findTableName(type))) {

            List<Get> gets = Lists.newArrayList();
            for (byte[] rowKey : rowKeyList) {
                Get get = GetUtils.initGet(rowKey,type);
                gets.add(get);
            }

            Result[] results = table.get(gets);

            return HBaseUtils.wrapResultList(type, results);
        } catch (Exception e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
    }

    @Override
    public <T> List<T> findList(byte[] startRow, byte[] endRow, Class<T> type) {
        Preconditions.checkNotNull(type, "[ERROR===>>>]class type can't be null");

        try (Table table = this.hBaseSource.getTable(HBaseUtils.findTableName(type));) {

            Scan scan = ScanUtils.initScan(startRow, endRow);

            List<T> resultList = Lists.newArrayList();
            ResultScanner scanner = table.getScanner(scan);

            Result result = null;
            while ((result = scanner.next()) != null) {
                T wrapResult = HBaseUtils.wrapResult(type, result);
                resultList.add(wrapResult);
            }
            return resultList;
        } catch (Exception e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
    }

    @Override
    public <T> List<T> findList(byte[] preRowkey, Class<T> type)  {
        Preconditions.checkNotNull(type, "[ERROR===>>>]class type can't be null");

        try (Table table = this.hBaseSource.getTable(HBaseUtils.findTableName(type));) {
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
        } catch (Exception e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
    }



    @Override
    public void delete(byte[] rowKey, Class<?> po)  {
        Preconditions.checkNotNull(po, "[ERROR===>>>]persistent po class can't be null");

        try (Table table = hBaseSource.getTable(HBaseUtils.findTableName(po))) {

            table.delete(HBaseUtils.wrapDelete(rowKey));
        } catch (Exception e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
    }


    @Override
    public void deleteList(List<byte[]> rowKeyList, Class<?> po)
             {
        Preconditions.checkNotNull(po, "[ERROR===>>>]persistent po class can't be null");

        try (Table table = this.hBaseSource.getTable(HBaseUtils.findTableName(po))) {

            List<Delete> deleteList = Lists.newArrayList();
            for (byte[] rowKey : rowKeyList) {
                deleteList.add(HBaseUtils.wrapDelete(rowKey));
            }
            table.delete(deleteList);
        } catch (Exception e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
    }

}
