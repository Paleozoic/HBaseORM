package com.maxplus1.test.hbase;

import com.maxplus1.hd_client.hbase.operations.HbaseAdmin;
import com.maxplus1.hd_client.hbase.operations.PageInfo;
import com.maxplus1.hd_client.hbase.operations.client.rtn_pojo.HbaseClient;
import com.maxplus1.test.pojo.User;
import com.maxplus1.test.base.BaseTest;
import com.maxplus1.test.model.OrmModel;
import com.maxplus1.test.utils.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Resource;
import java.util.List;
import java.util.Random;

/**
 * 测试：
 * （1）HbaseAdmin：listTables createTable deleteTable
 * （2）HbaseClient：put delete putList findListByPage deleteList
 */
@Slf4j
public class OrmTest extends BaseTest{

    @Resource
    private HbaseClient client;

    @Resource
    private HbaseAdmin admin;

    /**
     * TODO Fusion Insight HD 中，junit test会卡在Result result = table.get(get);原因不明。
     * 但是将用例放在web工程而不是spring test，则可以测试通过。
     * What the fuck!
     */
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

        //insert a new user
        User user = OrmModel.buildUser();
        client.put(user);
        log.info("insert {}:{}", user.getUserName(),JacksonUtils.obj2Json(user));

        //find user
        User findUser = client.find(user.getUuid(), User.class);
        log.info("find {}:{}",user.getUserName(),JacksonUtils.obj2Json(findUser));
        Assert.assertEquals(user,findUser);
        //delete user
        client.delete(user.getUuid(),User.class);
        log.info("delete {}:{}",user.getUserName(),JacksonUtils.obj2Json(findUser));
        Assert.assertEquals(null,client.find(user.getUuid(), User.class));

        //insert user list
        List<User> userList = OrmModel.buildUsers();
        client.putList(userList);
        log.info("insert {} user list success!",userList.size());

        // find list
        List<String> rowKeyList = OrmModel.getRowkeyList(userList);
        List<User> findUserList = client.findList(rowKeyList, User.class);
        log.info("findUserList,size is {}",findUserList.size());
        Assert.assertEquals(userList.size(),findUserList.size());

        //scan user
        OrmModel.sortList(rowKeyList);
        String startRow = rowKeyList.get(0);
        String endRow = rowKeyList.get(rowKeyList.size()-1);
        List<User> scanUserList = client.findList(startRow, endRow, User.class);
        log.info("scan list,size is {}",scanUserList.size());
        Assert.assertEquals(userList.size(),scanUserList.size()+1);//最后一个刚刚exclude

        // scan by page 初始化第一次查询
        PageInfo<User> pageInfo = new PageInfo<>(User.class);
        pageInfo.setPageSize(10);
        pageInfo.setStartRow(startRow);
        pageInfo.setStopRow(endRow);
        PageInfo<User> scanPage = client.findListByPage(pageInfo);
        log.info("scan by page,size is {}",scanPage.getDataSet().size());
        Assert.assertEquals(10,scanPage.getDataSet().size());
        Assert.assertEquals(startRow,scanPage.getDataSet().get(0).getUuid());
        Assert.assertEquals(rowKeyList.get(9),scanPage.getDataSet().get(9).getUuid());


        //模拟各种翻页
        Random random = new Random();
        //TODO 边界处理
        //因为只有5页，暂时不处理边界情况
        for(int i=0;i<500;i++){
            boolean flag = random.nextBoolean();
            if(flag&&pageInfo.getCurrentPage()>1){//向上翻页
                //向上翻一页
                pageInfo.setPrePageNo(pageInfo.getCurrentPage());
                pageInfo.setCurrentPage(pageInfo.getCurrentPage()-1);
                PageInfo<User> scanLastPage = client.findListByPage(pageInfo);
                log.info("last page is {},size is {}",scanLastPage.getCurrentPage(),scanLastPage.getDataSet().size());
                Assert.assertEquals(10,scanLastPage.getDataSet().size());
                Assert.assertEquals(rowKeyList.subList((pageInfo.getCurrentPage()-1)*10,pageInfo.getCurrentPage()*10),OrmModel.getAllUUid(scanLastPage.getDataSet()));
            }else if(!flag&&pageInfo.getCurrentPage()<5){//向下翻页
                //向下翻一页
                pageInfo.setPrePageNo(pageInfo.getCurrentPage());
                pageInfo.setCurrentPage(pageInfo.getCurrentPage()+1);
                PageInfo<User> scanNextPage = client.findListByPage(pageInfo);
                log.info("next page is {},size is {}",scanNextPage.getCurrentPage(),scanNextPage.getDataSet().size());
                Assert.assertEquals(10,scanNextPage.getDataSet().size());
                Assert.assertEquals(rowKeyList.subList((pageInfo.getCurrentPage()-1)*10,pageInfo.getCurrentPage()*10),OrmModel.getAllUUid(scanNextPage.getDataSet()));
            }else{
                log.info("nothing to do!");
                i--;
            }
        }

        log.info("findList by prerowkey starts!");
        User preUser1 = new User("abcdefg1","test1",1);
        User preUser2 = new User("abcdefg2","test2",2);
        User preUser3= new User("abcdefg3","test3",3);
        client.put(preUser1);
        client.put(preUser2);
        client.put(preUser3);
        List<User> preUserList = client.findList("abcdefg", User.class);
        Assert.assertEquals(preUserList.size(),3);
        Assert.assertTrue(preUserList.contains(preUser1));
        Assert.assertTrue(preUserList.contains(preUser2));
        Assert.assertTrue(preUserList.contains(preUser3));
        log.info("findList by prerowkey ends!");

        // delete list
        client.deleteList(rowKeyList,User.class);
        log.info("deleteList,size is {}",rowKeyList.size());
        //delete table
        admin.deleteTable("test:user");
        log.info("delete table :{}","test:user");
    }

}
