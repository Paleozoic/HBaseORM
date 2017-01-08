package com.maxplus1.hd_client.hbase.type.resolvers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by qxloo on 2016/12/26.
 */
public class IntegerTypeResolver extends TargetTypeResolver<Integer> {

    @Override
    public byte[] toBytes(Integer object) {
        Preconditions.checkArgument(accept(object), "target type can't be match Integer");

        return Bytes.toBytes(object);
    }

    @Override
    public Integer toObject(byte[] bytes) {
        return Bytes.toInt(bytes);
    }
}