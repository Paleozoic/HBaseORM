package com.maxplus1.test.hbase;

import com.maxplus1.hd_client.hbase.cache.TableColumnCacheManager;
import com.maxplus1.hd_client.hbase.operations.HbaseAdmin;
import com.maxplus1.hd_client.hbase.operations.beans.PageInfo;
import com.maxplus1.hd_client.hbase.operations.client.rtn_map.MapClient;
import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.beans.ColumnTypeMetaData;
import com.maxplus1.test.base.BaseTest;
import com.maxplus1.test.model.MapModel;
import com.maxplus1.test.utils.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 测试：
 * （1）HbaseAdmin：listTables createTable deleteTable
 * （2）MapClient：
 */
@Slf4j
public class MapTest extends BaseTest {

    @Resource
    private MapClient client;

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

        TableColumnCacheManager.put("test:user","info","user_name1", ColumnTypeMetaData.ColumnType.STRING);
        TableColumnCacheManager.put("test:user","info","age", ColumnTypeMetaData.ColumnType.INTEGER);

        //create table
        admin.createTable("test:user","info");
        log.info("create table :{},column family :{}","test:user","info");

        //insert a new user
        Map user = MapModel.buildUser();
        client.put("test:user",user);
        log.info("insert :{}", JacksonUtils.obj2Json(user));

        //find user
        Map findUser = client.find("test:user", user.get("rowkey")+"");
        log.info("find :{}",JacksonUtils.obj2Json(findUser));
        Assert.assertEquals(user,findUser);
        //delete user
        client.delete("test:user",user.get("rowkey")+"");
        log.info("delete :{}",JacksonUtils.obj2Json(findUser));
        Assert.assertEquals(null,client.find("test:user", user.get("rowkey")+""));

        //insert user list
        List<Map> userList = MapModel.buildUsers();
        client.putList("test:user",userList);
        log.info("insert {} user list success!",userList.size());

        // find list
        List<String> rowKeyList = MapModel.getRowkeyList(userList);
        List<Map> findUserList = client.findList("test:user",rowKeyList);
        log.info("findUserList,size is {}",findUserList.size());
        Assert.assertEquals(userList.size(),findUserList.size());

        //scan user
        MapModel.sortList(rowKeyList);
        String startRow = rowKeyList.get(0);
        String endRow = rowKeyList.get(rowKeyList.size()-1);
        List<Map> scanUserList = client.findList("test:user",startRow, endRow);
        log.info("scan list,size is {}",scanUserList.size());
        Assert.assertEquals(userList.size(),scanUserList.size()+1);//最后一个刚刚exclude

        // scan by page 初始化第一次查询
        PageInfo<Map> pageInfo = new PageInfo<>(Map.class);
        pageInfo.setPageSize(10);
        pageInfo.setStartRow(startRow);
        pageInfo.setStopRow(endRow);
        PageInfo<Map> scanPage = client.findListByPage("test:user",pageInfo);
        log.info("scan by page,size is {}",scanPage.getDataSet().size());
        Assert.assertEquals(10,scanPage.getDataSet().size());
        Assert.assertEquals(startRow,scanPage.getDataSet().get(0).get("rowkey"));
        Assert.assertEquals(rowKeyList.get(9),scanPage.getDataSet().get(9).get("rowkey"));


        //模拟各种翻页
        Random random = new Random();
        //TODO 边界处理
        //因为只有5页，暂时不处理边界情况
        for(int i=0;i<50;i++){
            if(random.nextBoolean()&&pageInfo.getCurrentPage()>1){//向上翻页
                //向上翻一页
                pageInfo.setPrePageNo(pageInfo.getCurrentPage());
                pageInfo.setCurrentPage(pageInfo.getCurrentPage()-1);
                PageInfo<Map> scanLastPage = client.findListByPage("test:user",pageInfo);
                log.info("last page is {},size is {}",scanLastPage.getCurrentPage(),scanLastPage.getDataSet().size());
                Assert.assertEquals(10,scanLastPage.getDataSet().size());
                Assert.assertEquals(rowKeyList.subList((pageInfo.getCurrentPage()-1)*10,pageInfo.getCurrentPage()*10),MapModel.getAllUUid(scanLastPage.getDataSet()));
            }else if(!random.nextBoolean()&&pageInfo.getCurrentPage()<5){//向下翻页
                //向下翻一页
                pageInfo.setPrePageNo(pageInfo.getCurrentPage());
                pageInfo.setCurrentPage(pageInfo.getCurrentPage()+1);
                PageInfo<Map> scanNextPage = client.findListByPage("test:user",pageInfo);
                log.info("next page is {},size is {}",scanNextPage.getCurrentPage(),scanNextPage.getDataSet().size());
                Assert.assertEquals(10,scanNextPage.getDataSet().size());
                Assert.assertEquals(rowKeyList.subList((pageInfo.getCurrentPage()-1)*10,pageInfo.getCurrentPage()*10),MapModel.getAllUUid(scanNextPage.getDataSet()));
            }else{
                log.info("nothing to do!");
                i--;
            }
        }

        // delete list
        client.deleteList("test:user",rowKeyList);
        log.info("deleteList,size is {}",rowKeyList.size());
        //delete table
        admin.deleteTable("test:user");
        log.info("delete table :{}","test:user");


    }

}
