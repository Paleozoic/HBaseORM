package com.maxplus1.test.hbase;

import com.maxplus1.hd_client.hbase.operations.HbaseAdmin;
import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.HbaseClient;
import com.maxplus1.test.base.BaseTest;
import com.maxplus1.test.model.ComplicateOrmModel;
import com.maxplus1.test.pojo.Manager;
import com.maxplus1.test.pojo.Manager2;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Resource;

/**
 * 测试自定义对象，拆分存取和序列化存取
 * Created by qxloo on 2016/12/31.
 */
@Slf4j
public class MyObjOrmTest extends BaseTest {

    @Resource
    private HbaseClient client;

    @Resource
    private HbaseAdmin admin;

    @Test
    public void test()  {
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            if(hTableDescriptor.getTableName().getNameAsString().equals("test:manager")){
                //delete table
                admin.deleteTable("test:manager");
                log.info("delete table :{}","test:manager");
            }
        }

        //create table
        admin.createTable("test:manager","info");
        log.info("create table :{},column family :{}","test:manager","info");

        //不拆分
        Manager2 manager2 = ComplicateOrmModel.buildManager2();
        client.put(manager2);

        Manager2 findManager2 = client.find(manager2.getUuid(),Manager2.class);
        Assert.assertEquals(manager2,findManager2);

        client.delete(findManager2.getUuid(),Manager2.class);

        //拆分对象
        Manager manager = ComplicateOrmModel.buildManager();
        client.put(manager);

        Manager findManager = client.find(manager.getUuid(),Manager.class);
        Assert.assertEquals(manager,findManager);

        client.delete(findManager.getUuid(),Manager.class);

        //嵌套自定义对象


        //delete table
        admin.deleteTable("test:manager");
        log.info("delete table :{}","test:dept");
    }
}
