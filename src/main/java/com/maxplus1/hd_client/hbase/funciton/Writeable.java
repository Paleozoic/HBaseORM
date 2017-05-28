package com.maxplus1.hd_client.hbase.funciton;

import java.util.List;

/**
 * 写操作接口
 * Created by qxloo on 2017/5/28.
 */
public interface Writeable {
    void delete(String rowKey, Class<?> po) ;

    void deleteList(List<String> rowKeyList, Class<?> po) ;

    <T> void put(T po) ;

    <T> void putList(List<T> poList) ;
}
