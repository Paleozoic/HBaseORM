package com.maxplus1.test.pojo;

import com.maxplus1.hd_client.hbase.annotation.Column;
import com.maxplus1.hd_client.hbase.annotation.RowKey;
import com.maxplus1.hd_client.hbase.annotation.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * Created by qxloo on 2016/12/31.
 */
@Table(name = "test:dept")
@Getter
@Setter
public class Dept {
    @RowKey
    private String deptId;
    @Column(family = "info", name = "deptName")
    private String deptName;
    @Column(family = "info", name = "userList")
    private  List<User> userList;

    public Dept() {
    }
    public Dept(String deptId,String deptName,List<User> userList) {
        this.deptId = deptId;
        this.deptName = deptName;
        this.userList = userList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Dept)) return false;

        Dept dept = (Dept) o;

        if (deptId != null ? !deptId.equals(dept.deptId) : dept.deptId != null) return false;
        if (deptName != null ? !deptName.equals(dept.deptName) : dept.deptName != null) return false;
        return userList != null ?  userList.equals(dept.userList) : dept.userList == null;

    }

    @Override
    public int hashCode() {
        int result = deptId != null ? deptId.hashCode() : 0;
        result = 31 * result + (deptName != null ? deptName.hashCode() : 0);
        result = 31 * result + (userList != null ? userList.hashCode() : 0);
        return result;
    }
}
