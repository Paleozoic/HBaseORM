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
@Table(name = "test:manager")
@Getter
@Setter
public class Manager {
    @RowKey
    private String uuid;
    @Column(family = "info", name = "deptName")
    private String deptName;
    @Column //不写列族信息表示拆分改类
    private  User user;

    public Manager() {
    }
    public Manager(String uuid,String deptName,User user) {
        this.uuid = uuid;
        this.deptName = deptName;
        this.user = user;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Manager)) return false;

        Manager manager = (Manager) o;

        if (uuid != null ? !uuid.equals(manager.uuid) : manager.uuid != null) return false;
        if (deptName != null ? !deptName.equals(manager.deptName) : manager.deptName != null) return false;
        return user != null ? user.equals(manager.user) : manager.user == null;

    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + (deptName != null ? deptName.hashCode() : 0);
        result = 31 * result + (user != null ? user.hashCode() : 0);
        return result;
    }
}
