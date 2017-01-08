/**
 * 
 */
package com.maxplus1.hd_client.hbase.funciton.rtn_pojo;

import java.util.List;

/**
 * 提供hbase基本功能，比如CRUD
 * 
 * @author zachary.zhang
 * @author edit by Paleo
 */
public interface BytesPersistent {

    <T> T find(byte[] rowKey, Class<T> type) ;

    <T> List<T> findList(List<byte[]> rowKeyList, Class<T> type) ;

    <T> List<T> findList(byte[] startRow, byte[] endRow, Class<T> type) ;

    <T> List<T> findList(byte[] preRowkey, Class<T> type) ;

    void delete(byte[] rowKey, Class<?> po) ;

    void deleteList(List<byte[]> rowKeyList, Class<?> po) ;

}
