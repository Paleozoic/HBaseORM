package com.maxplus1.hd_client.hbase.type.resolvers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;

/**
 * Created by qxloo on 2016/12/26.
 */
public class DateTypeResolver extends TargetTypeResolver<Date> {

    @Override
    public byte[] toBytes(Date object) {
        Preconditions.checkArgument(accept(object), "target type can't be match Date");
        return Bytes.toBytes(object.getTime());
    }

    @Override
    public Date toObject(byte[] bytes) {
        long date = Bytes.toLong(bytes);
        return new Date(date);
    }
}