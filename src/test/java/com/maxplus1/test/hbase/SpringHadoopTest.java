package com.maxplus1.test.hbase;

import com.maxplus1.hd_client.hbase.operations.HbaseAdmin;
import com.maxplus1.hd_client.hbase.operations.client.rtn_map.MapClient;
import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.HbaseTemplate;
import com.maxplus1.test.base.BaseTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.junit.Test;

import javax.annotation.Resource;

/**
 * 测试：
 * （1）HbaseAdmin：listTables createTable deleteTable
 * （2）HbaseTemplate：
 */
@Slf4j
public class SpringHadoopTest extends BaseTest {

    @Resource
    private HbaseTemplate client;

    @Resource
    private HbaseAdmin admin;

    @Test
    public void test()  {
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            if(hTableDescriptor.getTableName().getNameAsString().equals("test:user")){
                //delete table
                admin.deleteTable("test:user");
                log.info("delete table :{}","test:user");
            }
        }

        //create table
        admin.createTable("test:user","info");
        log.info("create table :{},column family :{}","test:user","info");


    }

}
