package com.maxplus1.hd_client.hbase.operations.client.spring_hbase;

import com.maxplus1.hd_client.hbase.utils.HBaseUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.springframework.util.Assert;

/**
 * Created by xiaolong.qiu on 2016/9/2.
 */
@Slf4j
public class HdRowMapper<T> implements RowMapper<T> {
    /** Logger available to subclasses */

    /** The class we are mapping to */
    private Class<T> mappedClass;

    public HdRowMapper(Class<T> mappedClass) {
        this.mappedClass = mappedClass;
    }


    @Override
    public T mapRow(Result result, int rowNumber) throws IllegalAccessException, InstantiationException {
        Assert.state(this.mappedClass != null, "Mapped class was not specified");
//        T mappedObject = BeanUtils.instantiate(this.mappedClass);
        T mappedObject = HBaseUtils.wrapResult(mappedClass,result);
        return mappedObject;
    }
}
