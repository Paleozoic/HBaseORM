package com.maxplus1.test.model;

import com.maxplus1.test.utils.NameUtils;
import com.maxplus1.test.utils.UUIDUtils;

import java.util.*;

/**
 * Created by qxloo on 2016/12/13.
 */
public class MapModel {
    public static List<Map> buildUsers(){
        List<Map> userList = new ArrayList<>();
        for(int i=0;i<500;i++){
            String uuid = UUIDUtils.getUUID();
            Map map =new HashMap();
            map.put("rowkey",uuid);
            map.put("info:user_name1",NameUtils.getName());
            map.put("info:age",(int)(Math.random()*100));
            userList.add(map);
        }
        return userList;
    }

    public static Map buildUser(){
        String uuid = UUIDUtils.getUUID();
        Map map =new HashMap();
        map.put("rowkey",uuid);
        map.put("info:user_name1",NameUtils.getName());
        map.put("info:age",(int)(Math.random()*100));
        return map;
    }

    public static List<String> getRowkeyList(List<Map> userList) {
        List<String> rowkeyList = new ArrayList<>();
        for(Map user:userList){
            rowkeyList.add(user.get("rowkey")+"");
        }
        return rowkeyList;
    }

    public static List<String> sortList(List<String> rowkeyList){
        Collections.sort(rowkeyList);
        return rowkeyList;
    }

    public static List<String> getAllUUid(List<Map> userList){
        List<String> strList = new ArrayList<>();
        for (Map user : userList) {
            strList.add(user.get("rowkey")+"");
        }
        return strList;
    }
}
