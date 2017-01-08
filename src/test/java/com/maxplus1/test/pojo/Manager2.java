package com.maxplus1.test.pojo;

import com.maxplus1.hd_client.hbase.annotation.Column;
import com.maxplus1.hd_client.hbase.annotation.RowKey;
import com.maxplus1.hd_client.hbase.annotation.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by qxloo on 2016/12/31.
 */
@Table(name = "test:manager")
@Getter
@Setter
public class Manager2 {
    @RowKey
    private String uuid;
    @Column(family = "info", name = "deptName")
    private String deptName;
    @Column(family = "info", name = "user")
    private  User user;

    public Manager2() {
    }
    public Manager2(String uuid,String deptName,User user) {
        this.uuid = uuid;
        this.deptName = deptName;
        this.user = user;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Manager2)) return false;

        Manager2 manager2 = (Manager2) o;

        if (uuid != null ? !uuid.equals(manager2.uuid) : manager2.uuid != null) return false;
        if (deptName != null ? !deptName.equals(manager2.deptName) : manager2.deptName != null) return false;
        return user != null ? user.equals(manager2.user) : manager2.user == null;

    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + (deptName != null ? deptName.hashCode() : 0);
        result = 31 * result + (user != null ? user.hashCode() : 0);
        return result;
    }
}
