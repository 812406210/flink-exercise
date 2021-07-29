package com.ywj.domains;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-07-06 18:31
 */
public class Person {
    public Person(){

    }
    public Person(String name , int age){
        this.name = name;
        this.age = age;
    }
    private String name;
    private int age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}