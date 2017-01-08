package com.maxplus1.hd_client.hbase.type.resolvers;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by qxloo on 2016/12/26.
 */
public class BooleanTypeResolver extends TargetTypeResolver<Boolean> {

    @Override
    public byte[] toBytes(Boolean object) {
        return Bytes.toBytes(object);
    }

    @Override
    public Boolean toObject(byte[] bytes) {
        return Bytes.toBoolean(bytes);
    }
}