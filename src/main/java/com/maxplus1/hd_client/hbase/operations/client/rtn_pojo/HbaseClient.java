package com.maxplus1.hd_client.hbase.operations.client.rtn_pojo;

import com.maxplus1.hd_client.hbase.funciton.Readable;
import com.maxplus1.hd_client.hbase.funciton.Writeable;
import com.maxplus1.hd_client.hbase.operations.PageInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * 面向用户接口
 * @author zachary.zhang
 * @author Paleo
 */
@Slf4j
@Component("com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.HbaseClient")
public class HbaseClient implements Readable,Writeable {

    @Resource
    private BytesClient bytesClient;

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
        bytesClient.put(po);
    }

    @Override
    public <T> void putList(List<T> poList)  {
        bytesClient.putList(poList);
    }

    @Override
    public <T> PageInfo<T> findListByPage(PageInfo<T> pageInfo, Filter... filters) {
        return bytesClient.findListByPage(pageInfo,filters);
    }

}
