package com.maxplus1.hd_client.hbase.utils;

import com.maxplus1.hd_client.hbase.operations.PageInfo;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by qxloo on 2016/11/28.
 */
public enum PageUtils {
    ;
    /**
     *
     * @param pageInfo
     * @param firstRowkey 结果集第一行的rowkey
     * @param lastRowkey 结果集最后一行的rowkey
     * @return
     */
    public static PageInfo changePageInfo(PageInfo pageInfo, byte[] firstRowkey, byte[] lastRowkey){
        if(pageInfo.getStartRow()==null||pageInfo.getStartRow().length==0){//第一条数据作为LastStartRow
            pageInfo.setLastStartRow(firstRowkey);
        }
        if(pageInfo.isPreviousPage()){
            firstRowkey[firstRowkey.length-1] = (byte) (firstRowkey[firstRowkey.length-1] - 1);
            pageInfo.setLastStartRow(firstRowkey);

        }else {
            pageInfo.setLastStartRow(pageInfo.getStartRow());
        }
        lastRowkey[lastRowkey.length-1] = (byte) (lastRowkey[lastRowkey.length-1] + 1);
        pageInfo.setStartRow(Bytes.add(lastRowkey, new byte[]{0}));
        return pageInfo;
    }
}
