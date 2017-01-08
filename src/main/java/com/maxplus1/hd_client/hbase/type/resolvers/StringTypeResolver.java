package com.maxplus1.hd_client.hbase.type.resolvers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by qxloo on 2016/12/26.
 */
public class StringTypeResolver extends TargetTypeResolver<String> {

    @Override
    public byte[] toBytes(String object) {
        Preconditions.checkArgument(accept(object), "target type can't be match String");

        return Bytes.toBytes(object);
    }

    @Override
    public String toObject(byte[] bytes) {
        return Bytes.toString(bytes);
    }
}