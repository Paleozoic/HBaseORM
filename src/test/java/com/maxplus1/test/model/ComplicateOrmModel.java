package com.maxplus1.test.model;

import com.maxplus1.test.pojo.Dept;
import com.maxplus1.test.pojo.Manager;
import com.maxplus1.test.pojo.Manager2;
import com.maxplus1.test.pojo.User;
import com.maxplus1.test.utils.UUIDUtils;

import java.util.List;
import java.util.Random;

/**
 * Created by qxloo on 2016/12/31.
 */
public class ComplicateOrmModel {

    private final static String[] deptNames = new String[]{"财务部","市场部","数据运营部","IT部"};
    private final static Random random = new Random();

    public static Dept buildDept(){
        List<User> users = OrmModel.buildUsers();
        Dept dept = new Dept(UUIDUtils.getUUID(),deptNames[random.nextInt(4)],users);
        return dept;
    }

    public static Manager buildManager(){
        User user = OrmModel.buildUser();
        Manager manager = new Manager(UUIDUtils.getUUID(),deptNames[random.nextInt(4)],user);
        return manager;
    }

    public static Manager2 buildManager2(){
        User user = OrmModel.buildUser();
        Manager2 manager2 = new Manager2(UUIDUtils.getUUID(),deptNames[random.nextInt(4)],user);
        return manager2;
    }
}
