package com.maxplus1.hd_client.hbase.funciton.rtn_map;

import com.maxplus1.hd_client.hbase.operations.beans.Column;
import com.maxplus1.hd_client.hbase.operations.beans.PageInfo;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import java.util.List;
import java.util.Map;

/**
 * Created by qxloo on 2016/11/26.
 */
@Deprecated
public interface MapPersistent {

    /**
     * 基础接口
     */
    List<Map> findList(String tableName, Scan scan, Filter ...filters) ;
    Map get(String tableName, Get get,Filter ...filters) ;
    List<Map> get(String tableName, List<Get> getList,Filter ...filters);

    Map find(String tableName , String rowKey) ;
    Map find(String tableName , List<Column> columnList,String rowKey) ;

    List<Map> findList(String tableName,List<String> rowKeyList,Filter ...filters) ;
    List<Map> findList(String tableName,List<Column> columnList,List<String> rowKeyList,Filter ...filters) ;

    List<Map>  findList(String tableName,String startRow, String endRow,Filter ...filters) ;
    List<Map>  findList(String tableName,List<Column> columnList,String startRow, String endRow,Filter ...filters) ;

    List<Map>  findList(String tableName,String preRowkey,Filter ...filters) ;
    List<Map>  findList(String tableName,List<Column> columnList,String preRowkey,Filter ...filters) ;

    void delete(String tableName,String rowKey) ;

    void deleteList(String tableName,List<String> rowKeyList) ;

    void put(String tableName,Map po) ;

    void putList(String tableName,List<Map> poList) ;

    /**
     * 分页
     * @param pageInfo
     * @return
     */
    PageInfo<Map> findListByPage(String tableName, PageInfo<Map> pageInfo, Filter... filters) ;
    PageInfo<Map> findListByPage(String tableName, List<Column> columnList,PageInfo<Map> pageInfo, Filter... filters) ;

}
