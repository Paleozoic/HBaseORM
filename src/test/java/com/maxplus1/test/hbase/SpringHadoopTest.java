package com.maxplus1.test.hbase;

import com.maxplus1.hd_client.hbase.operations.HbaseAdmin;
import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.HbaseTemplate;
import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.RowMapper;
import com.maxplus1.test.base.BaseTest;
import com.maxplus1.test.model.OrmModel;
import com.maxplus1.test.pojo.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
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

        /**
         * 其他接口不再测试，理论上是可以运行的
         */
        User user1 = OrmModel.buildUser();
        client.put("test:user",user1.getUuid(),"info","user_name1", Bytes.toBytes(user1.getUserName()));
        client.put("test:user",user1.getUuid(),"info","age", Bytes.toBytes(user1.getAge()));
        User getUser = client.get("test:user", user1.getUuid(), new RowMapper<User>() {
            @Override
            public User mapRow(Result result, int rowNum) throws Exception {
                User user = new User();
                user.setUuid(Bytes.toString(result.getRow()));
                user.setAge(Bytes.toLong(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("age"))));
                user.setUserName(Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("user_name1"))));
                return user;
            }
        });
        Assert.assertEquals(user1,getUser);

        //delete table
        admin.deleteTable("test:user");
        log.info("delete table :{}","test:user");
    }

}
