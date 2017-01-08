package com.maxplus1.hd_client.hbase.type.resolvers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by qxloo on 2016/12/26.
 */
public class FloatTypeResolver extends TargetTypeResolver<Float> {

    @Override
    public byte[] toBytes(Float object) {
        Preconditions.checkArgument(accept(object), "target type can't be match Float");

        return Bytes.toBytes(object);
    }

    @Override
    public Float toObject(byte[] bytes) {
        return Bytes.toFloat(bytes);
    }
}