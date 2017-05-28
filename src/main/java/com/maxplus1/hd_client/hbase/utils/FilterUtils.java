package com.maxplus1.hd_client.hbase.utils;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import java.util.Arrays;
import java.util.List;

/**
 * Created by qxloo on 2016/12/4.
 */
public enum  FilterUtils  {
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

    public static <T extends Query> T addFilters(T query, Filter... filters) {
        List<Filter> filterList = Lists.newArrayList();
        Filter filter = query.getFilter();
        if (filter != null) {
            /**
             * 如果FilterList多了一个null，会一直报这个错：
             2016-12-04 17:05:53,373:INFO hconnection-0x339d1500-shared--pool1-t1 (RpcRetryingCaller.java:142) - Call exception, tries=10, retries=35, started=38364 ms ago, cancelled=false, msg=row '20130701' on table 'aliyun:mfd_bank_shibor' at region=aliyun:mfd_bank_shibor,,1480469896789.c11dbc06b0070b77820a2fedff484597., hostname=hdnode,16020,1480428580174, seqNum=2
             */
            filterList.add(filter);
        }
        if (!FilterUtils.isAllNull(filters)) {
            filterList.addAll(Arrays.asList(filters));
        }
        if (filterList.size() > 0) {
            FilterList f = new FilterList(filterList.toArray(new Filter[filterList.size()]));
            query.setFilter(f);
        }
        return query;
    }
}
