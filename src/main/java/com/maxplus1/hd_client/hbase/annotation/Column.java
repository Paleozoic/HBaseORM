package com.maxplus1.hd_client.hbase.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * HBase的column的注解
 * 
 * @author zachary.zhang
 *
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {

    /**
     * operations column family
     * 
     * @return
     */
    String family() default "";

    /**
     * operations column name
     * 
     * @return
     */
    String name() default "";
}
