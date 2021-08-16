package com.ywj.entity.po;

import java.util.Date;

/**
 * @program: datacenter
 * @description: 测试实体
 * @author: yang
 * @create: 2020-10-17 09:25
 */

public class User {

    private Integer id;

    private String name;

    private Integer age;

    private Date createTime;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name == null ? null : name.trim();
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", createTime=" + createTime +
                '}';
    }
}