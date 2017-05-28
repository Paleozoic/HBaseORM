package com.maxplus1.hd_client.hbase.utils;

import com.maxplus1.hd_client.hbase.cache.TableDefinitionCacheManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by qxloo on 2016/11/30.
 */
@Slf4j
public enum GetUtils {
    ;

    public static Get initGet(byte[] rowkey, Class classType) {
        Get get = new Get(rowkey);
        return TableDefinitionCacheManager.buildQuery(get,classType,false);
    }

}
