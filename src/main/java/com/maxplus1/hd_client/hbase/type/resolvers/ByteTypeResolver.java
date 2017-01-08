package com.maxplus1.hd_client.hbase.type.resolvers;

import com.google.common.base.Preconditions;

/**
 * Created by qxloo on 2016/12/26.
 */
public class ByteTypeResolver extends TargetTypeResolver<Byte> {

    @Override
    public byte[] toBytes(Byte object) {
        Preconditions.checkArgument(accept(object), "target type can't be match Byte");
        return new byte[] {((Byte) object).byteValue()};
    }

    @Override
    public Byte toObject(byte[] bytes) {
        Preconditions.checkState(bytes != null && bytes.length > 1,
                "byte array is null or lenght <1");
        return bytes[0];
    }
}