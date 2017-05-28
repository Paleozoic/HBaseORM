package com.maxplus1.hd_client.hbase.funciton;

import java.util.List;

/**
 * 基于byte[]的写操作接口
 * Created by qxloo on 2017/5/28.
 */
public interface BytesWriteable {

    void put(Object po);

    <T> void putList(List<T> poList);

    void delete(byte[] rowKey, Class<?> po) ;

    void deleteList(List<byte[]> rowKeyList, Class<?> po) ;

}
