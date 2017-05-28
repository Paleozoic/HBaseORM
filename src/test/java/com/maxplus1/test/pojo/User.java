package com.maxplus1.test.pojo;

import com.maxplus1.hd_client.hbase.annotation.Column;
import com.maxplus1.hd_client.hbase.annotation.RowKey;
import com.maxplus1.hd_client.hbase.annotation.Table;
import lombok.*;

@Table(name = "test:user")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {

    @RowKey
    @Column(family = "info", name = "userId")
    private String uuid;

    @Column(family = "info", name = "user_name1")
    private String userName;

    @Column(family = "info")
    private long age;

}
