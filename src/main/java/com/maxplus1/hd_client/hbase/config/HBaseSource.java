package com.maxplus1.hd_client.hbase.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Getter
@Setter
public class HBaseSource {

    private Connection conn;

    private Configuration configuration;

    public HBaseSource(Configuration configuration) {
        this.configuration = configuration;
    }

    @PostConstruct
    public void initConn() throws IOException {
        this.conn = ConnectionFactory.createConnection(configuration);
    }

    @Override
    protected void finalize() throws Throwable {
        getConn().close();
    }

    /**
     * 根据table name 返回HBase Table对象，可以执行CRUD操作 使用完毕之后，需要调用Table.close()
     *
     * @param tableName
     * @return Table
     */
    public Table getTable(String tableName) throws IOException {
        return this.getConn().getTable(TableName.valueOf(tableName));
    }

    public Admin getAdmin() throws IOException {
        return this.getConn().getAdmin();
    }

    /**
     * 关闭table
     *
     * @param table
     */
    public void closeTable(Table table) throws IOException {
        if (table != null) {
            table.close();
        }
    }

}
