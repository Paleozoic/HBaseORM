package com.maxplus1.test.pojo;

import com.maxplus1.hd_client.hbase.annotation.Column;
import com.maxplus1.hd_client.hbase.annotation.RowKey;
import com.maxplus1.hd_client.hbase.annotation.Table;
import lombok.*;

/**
 * Created by qxloo on 2016/12/31.
 */
@Table(name = "test:manager")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Manager2 {
    @RowKey
    private String uuid;
    @Column(family = "info", name = "deptName")
    private String deptName;
    @Column(family = "info", name = "user")
    private  User user;
}
