package com.maxplus1.hd_client.hbase.utils;

/**
 * Created by qxloo on 2017/1/3.
 */
public enum SysUtils {
    ;
    public static boolean isJavaClass(Class<?> clz) {
        return clz != null && clz.getClassLoader() == null;
    }
}
