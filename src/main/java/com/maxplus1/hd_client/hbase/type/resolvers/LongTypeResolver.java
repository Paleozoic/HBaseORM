package com.maxplus1.hd_client.hbase.type.resolvers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by qxloo on 2016/12/26.
 */
public class LongTypeResolver extends TargetTypeResolver<Long> {

    @Override
    public byte[] toBytes(Long object) {
        Preconditions.checkArgument(accept(object), "target type can't be match Long");

        return Bytes.toBytes(object);
    }

    @Override
    public Long toObject(byte[] bytes) {
        return Bytes.toLong(bytes);
    }
}