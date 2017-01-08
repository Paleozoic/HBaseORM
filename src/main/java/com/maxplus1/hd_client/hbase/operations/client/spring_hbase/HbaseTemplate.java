package com.maxplus1.hd_client.hbase.operations.client.spring_hbase;

import com.maxplus1.hd_client.hbase.config.HBaseSource;
import com.maxplus1.hd_client.hbase.exception.HbaseClientException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

/**
 * Created by qxloo on 2016/12/2.
 */
@Component("com.maxplus1.hd_client.hbase.operations.client.spring_hbase.HbaseTemplate")
public class HbaseTemplate implements HbaseOperations{

    @Resource
    private HBaseSource hBaseSource;

    @Override
    public <T> T execute(String tableName, TableCallback<T> action)  {
        Assert.notNull(action, "Callback object must not be null");
        Assert.notNull(tableName, "No table specified");
        Table table = null;
        try {
            table = this.hBaseSource.getTable(tableName);
            T result = action.doInTable(table);
            return result;
        } catch (Throwable e) {
            throw new HbaseClientException(e);
        } finally {
            releaseTable(table);
        }
    }

    private void releaseTable(Table table)  {
        if(table!=null){
            try {
                table.close();
            } catch (IOException e) {
                throw new HbaseClientException(e);
            }
        }
    }


    @Override
    public <T> T find(String tableName, String family, final ResultsExtractor<T> action)  {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));
        return find(tableName, scan, action);
    }

    @Override
    public <T> T find(String tableName, String family, String qualifier, final ResultsExtractor<T> action)  {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        return find(tableName, scan, action);
    }

    @Override
    public <T> T find(String tableName, final Scan scan, final ResultsExtractor<T> action)  {
        return execute(tableName, new TableCallback<T>() {
            @Override
            public T doInTable(Table table) throws Throwable {
                ResultScanner scanner = table.getScanner(scan);
                try {
                    return action.extractData(scanner);
                } finally {
                    scanner.close();
                }
            }
        });
    }

    @Override
    public <T> List<T> find(String tableName, String family, final RowMapper<T> action)  {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));
        return find(tableName, scan, action);
    }

    @Override
    public <T> List<T> find(String tableName, String family, String qualifier, final RowMapper<T> action)  {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        return find(tableName, scan, action);
    }

    @Override
    public <T> List<T> find(String tableName, final Scan scan, final RowMapper<T> action)  {
        return find(tableName, scan, new RowMapperResultsExtractor<T>(action));
    }

    @Override
    public <T> T get(String tableName, String rowName, final RowMapper<T> mapper)  {
        return get(tableName, rowName, null, null, mapper);
    }

    @Override
    public <T> T get(String tableName, String rowName, String familyName, final RowMapper<T> mapper)  {
        return get(tableName, rowName, familyName, null, mapper);
    }

    @Override
    public <T> T get(String tableName, final String rowName, final String familyName, final String qualifier, final RowMapper<T> mapper)  {
        return execute(tableName, new TableCallback<T>() {
            @Override
            public T doInTable(Table table) throws Throwable {
                Get get = new Get(Bytes.toBytes(rowName));
                if (familyName != null) {
                    byte[] family = Bytes.toBytes(familyName);

                    if (qualifier != null) {
                        get.addColumn(family, Bytes.toBytes(qualifier));
                    }
                    else {
                        get.addFamily(family);
                    }
                }
                Result result = table.get(get);
                return mapper.mapRow(result, 0);
            }
        });
    }

    @Override
    public void put(String tableName, final String rowName, final String familyName, final String qualifier, final byte[] value)  {
        Assert.hasLength(rowName);
        Assert.hasLength(familyName);
        Assert.hasLength(qualifier);
        Assert.notNull(value);
        execute(tableName, new TableCallback<Object>() {
            @Override
            public Object doInTable(Table table) throws Throwable {
                Put put = new Put(Bytes.toBytes(rowName));
                put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(qualifier),value);
                table.put(put);
                return null;
            }
        });
    }

    @Override
    public void delete(String tableName, final String rowName, final String familyName)  {
        delete(tableName, rowName, familyName, null);
    }

    @Override
    public void delete(String tableName, final String rowName, final String familyName, final String qualifier)  {
        Assert.hasLength(rowName);
        Assert.hasLength(familyName);
        execute(tableName, new TableCallback<Object>() {
            @Override
            public Object doInTable(Table table) throws Throwable {
                Delete delete = new Delete(Bytes.toBytes(rowName));
                byte[] family = Bytes.toBytes(familyName);
                if (qualifier != null) {
                    delete.addColumn(family, Bytes.toBytes(qualifier));
                }
                else {
                    delete.addFamily(family);
                }
                table.delete(delete);
                return null;
            }
        });
    }
}
