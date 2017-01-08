package com.maxplus1.test.utils;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by xiaolong.qiu on 2016/4/29.
 */

@Slf4j
public enum JacksonUtils {
    ;

    private static ObjectMapper mapper = new ObjectMapper();
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    /**
     * 转换对象为json数据
     *
     * @param object
     * @return
     */
    public static String obj2Json(Object object) {
        if (object == null) return "";
        try {
            mapper.getSerializationConfig().with(formatter);
            return mapper.writeValueAsString(object);
        } catch (Exception e) {
            log.warn("write to json string error:" + object, e);
            return "";
        }
    }


    /**
     * 转换json数据为对象
     *
     * @param json
     * @param clazz
     * @return
     */
    public static <T> T json2Obj(String json, Class<T> clazz) {
        if (isEmpty(json)) {
            return null;
        }
        try {
            mapper.getSerializationConfig().with(formatter);
            return mapper.readValue(json, clazz);
        } catch (Exception e) {
            log.warn("parse json string error:" + json, e);
            return null;
        }
    }

    /**
     * 转换json数据为对象列表
     *
     * @param json
     * @param classOfT
     * @return
     */
    public static <T> List<T> json2List(String json, Class<T> classOfT) {
        List<T> objList = null;
        try {
            mapper.getSerializationConfig().with(formatter);
            JavaType t = mapper.getTypeFactory().constructCollectionType(List.class, classOfT);
            objList = mapper.readValue(json, t);
        } catch (Exception e) {
        }
        return objList;
    }


    /**
     * 转换json数据为对象
     *
     * @param json
     * @return
     */
    public static Map json2Map(String json) {
        if (!isEmpty(json)) {
            return json2Obj(json, Map.class);
        } else {
            return null;
        }
    }

    private static boolean isEmpty(String str){
        return str==null||str.length()==0;
    }
}
