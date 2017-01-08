package com.maxplus1.hd_client.hbase.utils;

import org.apache.hadoop.hbase.NamespaceDescriptor;

/**
 * Created by qxloo on 2016/12/1.
 */
public enum  AdminUtils {
        ;
    public static boolean namespaceExists(String namespace,NamespaceDescriptor[] namespaceDescriptors){
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            if(namespace.equals(namespaceDescriptor.getName())){
                return true;
            }
        }
        return false;
    }

}
