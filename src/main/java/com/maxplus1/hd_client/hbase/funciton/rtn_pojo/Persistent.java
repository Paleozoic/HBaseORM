/**
 * 
 */
package com.maxplus1.hd_client.hbase.funciton.rtn_pojo;

import com.maxplus1.hd_client.hbase.operations.beans.PageInfo;
import org.apache.hadoop.hbase.filter.Filter;

import java.util.List;

/**
 * 提供hbase基本功能，比如CRUD
 * 
 * @author zachary.zhang
 * @author edit by Paleo
 */
public interface Persistent {

    <T> T find(String rowKey, Class<T> type) ;

    <T> List<T> findList(List<String> rowKeyList, Class<T> type) ;

    <T> List<T> findList(String startRow, String endRow, Class<T> type) ;

    <T> List<T> findList(String preRowkey, Class<T> type) ;

    void delete(String rowKey, Class<?> po) ;

    void deleteList(List<String> rowKeyList, Class<?> po) ;

    <T> void put(T po) ;

    <T> void putList(List<T> poList) ;

    <T> PageInfo<T> findListByPage(PageInfo<T> pageInfo, Filter... filters) ;

}
