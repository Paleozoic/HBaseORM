package com.maxplus1.test.pojo;

import com.maxplus1.hd_client.hbase.annotation.Column;
import com.maxplus1.hd_client.hbase.annotation.RowKey;
import com.maxplus1.hd_client.hbase.annotation.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Table(name = "test:user")
public class User {

    @RowKey
    @Column(family = "info", name = "userId")
    private String uuid;

    @Column(family = "info", name = "user_name1")
    private String userName;

    @Column(family = "info")
    private long age;

    public User(){
    }

    public User(String uuid,String userName,long age){
        this.uuid = uuid;
        this.userName = userName;
        this.age = age;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof User)) return false;

        User user = (User) o;

        if (age != user.age) return false;
        if (uuid != null ? !uuid.equals(user.uuid) : user.uuid != null) return false;
        return userName != null ? userName.equals(user.userName) : user.userName == null;

    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + (userName != null ? userName.hashCode() : 0);
        result = 31 * result + (int) (age ^ (age >>> 32));
        return result;
    }
}
