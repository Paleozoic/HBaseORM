package com.maxplus1.hd_client.hbase.type.rtn_pojo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.maxplus1.hd_client.hbase.type.resolvers.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;

/**
 * @author zachary.zhang
 * @author edit by Paleo
 */
@Component("com.maxplus1.hd_client.hbase.type.rtn_pojo.TypeResolverFactory")
public class TypeResolverFactory {


    private static Map<Class<?>, TypeResolver<?>> defaultResolvers = Maps.newHashMap();

    /**
     * Java集合类型序列化为json
     */
    private static SerializeTypeResolver serializeTypeResolver;
    @Resource
    private  SerializeTypeResolver privateSerializeTypeResolver;
    /**
     * 通过Void.class来表示自定义对象
     * （或者自定义一个无用的class来表示）
     * 根据注解不同进行处理：
     * （1）自定义对象序列化
     * （2）拆分对象写进hbase 1.根据自定义对象本身的注解写进hbase or 2.使用当前类的rowkey以及tableName
     */

    static {
        defaultResolvers.put(String.class, new StringTypeResolver());
        /**
         * date类型转化为时间戳
         */
        defaultResolvers.put(Date.class, new DateTypeResolver());

        defaultResolvers.put(Boolean.class, new BooleanTypeResolver());
        defaultResolvers.put(boolean.class, new BooleanTypeResolver());

        defaultResolvers.put(Byte.class, new ByteTypeResolver());
        defaultResolvers.put(byte.class, new ByteTypeResolver());

        defaultResolvers.put(Short.class, new ShortTypeResolver());
        defaultResolvers.put(short.class, new ShortTypeResolver());

        defaultResolvers.put(Integer.class, new IntegerTypeResolver());
        defaultResolvers.put(int.class, new IntegerTypeResolver());

        defaultResolvers.put(Long.class, new LongTypeResolver());
        defaultResolvers.put(long.class, new LongTypeResolver());

        defaultResolvers.put(Float.class, new FloatTypeResolver());
        defaultResolvers.put(float.class, new FloatTypeResolver());

        defaultResolvers.put(Double.class, new DoubleTypeResolver());
        defaultResolvers.put(double.class, new DoubleTypeResolver());
    }

    public static TypeResolver<?> getResolver(Class<?> type) {
        Preconditions.checkNotNull(type, "type can't be null");
        TypeResolver typeResolver = defaultResolvers.get(type);
        if(typeResolver==null){
            if(type==List.class||type==Map.class||type==Set.class){
                typeResolver = serializeTypeResolver;
            }else{//默认是序列化处理
                typeResolver = serializeTypeResolver;
            }
        }
        return typeResolver;
    }

    public void setSerializeTypeResolver(SerializeTypeResolver serializeTypeResolver) {
        TypeResolverFactory.serializeTypeResolver = serializeTypeResolver;
    }

    @PostConstruct
    public void PostConstruct() {
        serializeTypeResolver = this.privateSerializeTypeResolver;
    }
}
