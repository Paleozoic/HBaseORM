package com.maxplus1.hd_client.hbase.type.resolvers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by qxloo on 2016/12/26.
 */
public class DoubleTypeResolver extends TargetTypeResolver<Double> {

    @Override
    public byte[] toBytes(Double object) {
        Preconditions.checkArgument(accept(object), "target type can't be match Double");
        return Bytes.toBytes(object);
    }

    @Override
    public Double toObject(byte[] bytes) {
        return Bytes.toDouble(bytes);
    }

}