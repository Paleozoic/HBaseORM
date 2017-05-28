package com.maxplus1.test.pojo;

import com.maxplus1.hd_client.hbase.annotation.Column;
import com.maxplus1.hd_client.hbase.annotation.RowKey;
import com.maxplus1.hd_client.hbase.annotation.Table;
import lombok.*;

import java.util.List;

/**
 * Created by qxloo on 2016/12/31.
 */
@Table(name = "test:dept")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Dept {
    @RowKey
    private String deptId;
    @Column(family = "info", name = "deptName")
    private String deptName;
    @Column(family = "info", name = "userList")
    private  List<User> userList;


}
