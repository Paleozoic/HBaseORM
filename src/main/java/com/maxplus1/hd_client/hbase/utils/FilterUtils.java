package com.maxplus1.hd_client.hbase.utils;

import org.apache.hadoop.hbase.filter.Filter;

/**
 * Created by qxloo on 2016/12/4.
 */
public enum FilterUtils {
    ;
    /**
     * 如果调用了isAllNull(null) 也会得到一个长度为1的filters，但是里面元素为null
     * 这个方法是为了排除此种情况
     * @param filters
     * @return
     */
    public static boolean isAllNull(Filter... filters){
        if(filters==null||filters.length<=0){
            return true;
        }else{
            for (Filter filter : filters) {
                if(filter!=null){
                    return false;
                }
            }
            return true;
        }
    }
}
