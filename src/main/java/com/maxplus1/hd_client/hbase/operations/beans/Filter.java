package com.maxplus1.hd_client.hbase.operations.beans;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.filter.CompareFilter;

/**
 * Created by qxloo on 2016/11/26.
 */
@Setter
@Getter
@Deprecated
public class Filter {
    private CompareFilter compareFilter;
    private CompareFilter.CompareOp compareOp;
    private String keyword;
}
