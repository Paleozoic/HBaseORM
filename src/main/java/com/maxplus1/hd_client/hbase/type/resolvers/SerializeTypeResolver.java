package com.maxplus1.hd_client.hbase.type.resolvers;

import com.maxplus1.hd_client.hbase.serializer.HbaseSerializer;

/**
 * 需要序列化处理的对象：
 * Java集合类型处理
 * 自定义对象的处理
 * Created by qxloo on 2016/12/26.
 */
public class SerializeTypeResolver<T> extends TargetTypeResolver<T> {

    private HbaseSerializer<T> hbaseSerializer;

    @Override
    public byte[] toBytes(T object) {
        return hbaseSerializer.serialize(object);
    }

    @Override
    public T toObject(byte[] bytes) {
        return hbaseSerializer.deserialize(bytes);
    }

    public HbaseSerializer<T> getHbaseSerializer() {
        return hbaseSerializer;
    }

    public void setHbaseSerializer(HbaseSerializer<T> hbaseSerializer) {
        this.hbaseSerializer = hbaseSerializer;
    }
}
