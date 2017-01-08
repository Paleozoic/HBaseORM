package com.maxplus1.hd_client.hbase.type.resolvers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by qxloo on 2016/12/26.
 */
public class ShortTypeResolver extends TargetTypeResolver<Short> {

    @Override
    public byte[] toBytes(Short object) {
        Preconditions.checkArgument(accept(object), "target type can't be match Short");

        return Bytes.toBytes(object);
    }

    @Override
    public Short toObject(byte[] bytes) {
        return Bytes.toShort(bytes);
    }
}