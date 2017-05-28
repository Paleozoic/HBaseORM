package com.maxplus1.test.pojo;

import com.maxplus1.hd_client.hbase.annotation.Column;
import com.maxplus1.hd_client.hbase.annotation.RowKey;
import com.maxplus1.hd_client.hbase.annotation.Table;
import lombok.*;

import java.util.List;

/**
 * Created by qxloo on 2016/12/31.
 */
@Table(name = "test:manager")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Manager {
    @RowKey
    private String uuid;
    @Column(family = "info", name = "deptName")
    private String deptName;
    @Column //不写列族信息表示拆分改类
    private  User user;



}
