package com.maxplus1.hd_client.hbase.operations.client.spring_hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

class RowMapperResultsExtractor<T> implements ResultsExtractor<List<T>> {

    private final RowMapper<T> rowMapper;

    /**
     * Create a new RowMapperResultSetExtractor.
     * @param rowMapper the RowMapper which creates an object for each row
     */
    public RowMapperResultsExtractor(RowMapper<T> rowMapper) {
        Assert.notNull(rowMapper, "RowMapper is required");
        this.rowMapper = rowMapper;
    }

    public List<T> extractData(ResultScanner results) throws Exception {
        List<T> rs = new ArrayList<T>();
        int rowNum = 0;
        for (Result result : results) {
            rs.add(this.rowMapper.mapRow(result, rowNum++));
        }
        return rs;
    }
}
