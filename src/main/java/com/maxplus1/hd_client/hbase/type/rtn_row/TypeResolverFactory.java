package com.maxplus1.hd_client.hbase.type.rtn_row;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.beans.ColumnTypeMetaData;
import com.maxplus1.hd_client.hbase.type.resolvers.*;

import java.util.Map;

import static com.maxplus1.hd_client.hbase.operations.client.spring_hbase.beans.ColumnTypeMetaData.ColumnType.*;

/**
 * @author Paleo
 */
@Deprecated
public class TypeResolverFactory {


    private static Map<ColumnTypeMetaData.ColumnType, TypeResolver<?>> defaultResolvers = Maps.newHashMap();

    static {
        defaultResolvers.put(STRING, new StringTypeResolver());
        /**
         * date类型转化为时间戳
         */
        defaultResolvers.put(DATE, new DateTypeResolver());

        defaultResolvers.put(BOOLEAN, new BooleanTypeResolver());

        defaultResolvers.put(BYTE, new ByteTypeResolver());

        defaultResolvers.put(SHORT, new ShortTypeResolver());

        defaultResolvers.put(INTEGER, new IntegerTypeResolver());

        defaultResolvers.put(LONG, new LongTypeResolver());

        defaultResolvers.put(FLOAT, new FloatTypeResolver());

        defaultResolvers.put(DOUBLE, new DoubleTypeResolver());

        /**
         * Java集合类型序列化为json
         */
        defaultResolvers.put(JAVA_SET, new SerializeTypeResolver());
        /**
         * 通过Void.class来表示自定义对象
         * （或者自定义一个无用的class来表示）
         * 根据注解不同进行处理：
         * （1）自定义对象序列化
         * （2）拆分对象写进hbase 1.根据自定义对象本身的注解写进hbase or 2.使用当前类的rowkey以及tableName
         */
    }

    public static TypeResolver<?> getResolver(ColumnTypeMetaData.ColumnType type) {
        Preconditions.checkNotNull(type, "type can't be null");
        return defaultResolvers.get(type);
    }


}
