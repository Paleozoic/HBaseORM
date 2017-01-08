package com.maxplus1.test.hbase;

import com.maxplus1.hd_client.hbase.operations.HbaseAdmin;
import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.HbaseClient;
import com.maxplus1.test.base.BaseTest;
import com.maxplus1.test.model.ComplicateOrmModel;
import com.maxplus1.test.pojo.Dept;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Resource;

/**
 * 测试java集合的存取（序列化）
 * Created by qxloo on 2016/12/31.
 */
@Slf4j
public class JavaSetOrmTest extends BaseTest {

    @Resource
    private HbaseClient client;

    @Resource
    private HbaseAdmin admin;

    @Test
    public void test()  {
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            if(hTableDescriptor.getTableName().getNameAsString().equals("test:dept")){
                //delete table
                admin.deleteTable("test:dept");
                log.info("delete table :{}","test:dept");
            }
        }

        //create table
        admin.createTable("test:dept","info");
        log.info("create table :{},column family :{}","test:dept","info");

        Dept dept = ComplicateOrmModel.buildDept();
        client.put(dept);

        Dept findDept = client.find(dept.getDeptId(),Dept.class);
        Assert.assertEquals(dept,findDept);



        //delete table
        admin.deleteTable("test:dept");
        log.info("delete table :{}","test:dept");
    }
}
