package com.maxplus1.hd_client.hbase.funciton;

import com.maxplus1.hd_client.hbase.operations.PageInfo;
import org.apache.hadoop.hbase.filter.Filter;

import java.util.List;

/**
 * 基于byte[]的读操作接口
 * Created by qxloo on 2017/5/28.
 */
public interface BytesReadable {
    <T> T find(byte[] rowKey, Class<T> type) ;

    <T> List<T> findList(List<byte[]> rowKeyList, Class<T> type) ;

    <T> List<T> findList(byte[] startRow, byte[] endRow, Class<T> type) ;

    <T> List<T> findList(byte[] preRowkey, Class<T> type) ;

    <T> PageInfo<T> findListByPage(PageInfo<T> pageInfo, Filter... filters);
}
