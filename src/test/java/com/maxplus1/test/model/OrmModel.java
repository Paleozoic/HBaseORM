package com.maxplus1.test.model;

import com.maxplus1.test.pojo.User;
import com.maxplus1.test.utils.NameUtils;
import com.maxplus1.test.utils.UUIDUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by qxloo on 2016/12/13.
 */
public class OrmModel {
    public static List<User> buildUsers(){
        List<User> userList = new ArrayList<>();
        for(int i=0;i<500;i++){
            String uuid = UUIDUtils.getUUID();
            User user = new User(uuid, NameUtils.getName(),(int)(Math.random()*100));
            userList.add(user);
        }
        return userList;
    }

    public static User buildUser(){
        String uuid = UUIDUtils.getUUID();
        User user = new User(uuid, NameUtils.getName(),(int)(Math.random()*100));
        return user;
    }

    public static List<String> getRowkeyList(List<User> userList) {
        List<String> rowkeyList = new ArrayList<>();
        for(User user:userList){
            rowkeyList.add(user.getUuid());
        }
        return rowkeyList;
    }

    public static List<String> sortList(List<String> rowkeyList){
        Collections.sort(rowkeyList);
        return rowkeyList;
    }

    public static List<String> getAllUUid(List<User> userList){
        List<String> strList = new ArrayList<>();
        for (User user : userList) {
            strList.add(user.getUuid());
        }
        return strList;
    }
}
