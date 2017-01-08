package com.maxplus1.hd_client.hbase.operations;

import com.maxplus1.hd_client.hbase.config.HBaseSource;
import com.maxplus1.hd_client.hbase.exception.HbaseClientException;
import com.maxplus1.hd_client.hbase.funciton.Schema;
import com.maxplus1.hd_client.hbase.utils.AdminUtils;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;


@Slf4j
@Component("com.maxplus1.hd_client.hbase.operations.HbaseAdmin")
public class HbaseAdmin implements Schema {

    @Resource
    private HBaseSource hBaseSource;

    @Override
    public void createTable(String tableName, String... columnFamily) {
        try (Admin admin = this.hBaseSource.getAdmin()) {// auto close hbaseadmin

            TableName tn = TableName.valueOf(tableName);
            if(admin.tableExists(tn)){
                log.warn("[WARN==>>>]table {} exists!", tableName);
            }else{
                String namespace = tn.getNamespaceAsString();
                NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
                if(!AdminUtils.namespaceExists(namespace,namespaceDescriptors)){//判断表空间是否存在
                    NamespaceDescriptor namespaceDesc = NamespaceDescriptor.create(namespace).build();
                    admin.createNamespace(namespaceDesc);
                    log.warn("[WARN===>>>]namespace {} exists!", namespace);
                }
                HTableDescriptor tableDesc = new HTableDescriptor(tn);
                for (String cf : columnFamily) {
                    tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes(cf)));
                }
                // 添加协处理器
                // tableDesc.addCoprocessor(className, jarFilePath, priority, kvs);
                admin.createTable(tableDesc);
            }
        } catch (Exception e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
    }

    @Override
    public void deleteTable(String tableName)  {
        try (Admin admin = this.hBaseSource.getAdmin()) {// auto close hbaseadmin
            TableName hTableName = TableName.valueOf(tableName);
            admin.disableTable(hTableName);
            admin.deleteTable(hTableName);
        } catch (Exception e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
    }

    @Override
    public HTableDescriptor[]  listTables()  {
        try (Admin admin = this.hBaseSource.getAdmin()) {// auto close hbaseadmin
            HTableDescriptor[] hTableDescriptor =  admin.listTables();
            return hTableDescriptor;
        } catch (Exception e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
    }

    @Override
    public HTableDescriptor[]  listTables(String regex)  {
        try (Admin admin = this.hBaseSource.getAdmin()) {// auto close hbaseadmin
            HTableDescriptor[] hTableDescriptor =  admin.listTables(regex);
            return hTableDescriptor;
        } catch (Exception e) {
            log.error("[ERROR===>>>]", e);
            throw new HbaseClientException(e);
        }
    }

}
