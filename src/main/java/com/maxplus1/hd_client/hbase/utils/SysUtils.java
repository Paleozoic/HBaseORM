package com.maxplus1.hd_client.hbase.utils;

/**
 * Created by qxloo on 2017/1/3.
 */
public enum SysUtils {
    ;

    /**
     * https://stackoverflow.com/questions/8703678/how-can-i-check-if-a-class-belongs-to-java-jdk
     * 判断clz是否是JDK的类
     * @param clz
     * @return
     */
    public static boolean isJavaClass(Class<?> clz) {
        return clz != null && clz.getClassLoader() == null;
    }
}
