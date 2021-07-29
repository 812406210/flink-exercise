package com.canal.phonix.phonixUtils;

import org.springframework.stereotype.Component;

import java.sql.*;

/**
 * @program: hushuo-cdh
 * @description: phoenix配置文件
 * @author: yang
 * @create: 2020-11-03 14:11
 */
@Component
public class PhoenixUtil {

    public Connection getConnection()  {
        Connection conn = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            //这里配置zookeeper地址，可单个，也可多个，可以是域名或者ip
            String url="jdbc:phoenix:localhost:2181";
            conn= DriverManager.getConnection(url);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }catch (SQLException e){
            e.printStackTrace();
        }
        return conn;
    }

    /**创建数据表*/
    public void createTable(String tableName) throws Exception {
        Connection conn = this.getConnection();
        Statement statement = conn.createStatement();
        String sql="create table "+tableName+" (mykey integer not null primary key,mycolumn varchar)";
        statement.executeUpdate(sql);
        conn.commit();
        System.out.println("创建数据表成功！");
        conn.close();
        statement.close();
    }

    /**单条插入数据*/
    public void upsert(String sql) throws SQLException {
        Connection conn = this.getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate(sql);
        conn.commit();
        System.out.println("数据已插入！");
        conn.close();
        statement.close();
    }

    /**删除数据*/
    public void delete(String sql) throws SQLException {
        Connection conn = this.getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate(sql);
        conn.commit();
        System.out.println("删除数据成功！");
        conn.close();
        statement.close();
    }

    /**检索数据表中的记录*/
    public void readAll(String sql) throws SQLException {
        Connection conn = this.getConnection();
        Statement statement = conn.createStatement();
        long time=System.currentTimeMillis();
        ResultSet rs = statement.executeQuery(sql);
        conn.commit();
        while (rs.next()){
            //获取字段值
            int mykey = rs.getInt("mykey");
            //获取字段值
            String mycolumn = rs.getString("mycolumn");
            System.out.println("mykey:"+mykey+"\t"+"mycolumn:"+mycolumn);
        }
        rs.close();
        conn.close();
        statement.close();
    }

    /**检索数据表中的记录*/
    public void readByKey(String sql) throws SQLException {
        Connection conn = this.getConnection();
        Statement statement = conn.createStatement();
        long time=System.currentTimeMillis();
        ResultSet rs = statement.executeQuery(sql);
        conn.commit();
        while (rs.next()){
            //获取字段值
            int mykey = rs.getInt("mykey");
            //获取字段值
            String mycolumn = rs.getString("mycolumn");
            System.out.println("mykey:"+mykey+"\t"+"mycolumn:"+mycolumn);
        }
        rs.close();
        conn.close();
        statement.close();
    }

    public static void main(String[] args) throws SQLException {
        //PhoenixUtil.createTable("test");

        String readAllSql = "select * from TEST";
        //PhoenixUtil.readAll(readAllSql);
        String insertSql = "upsert into test values(2,'test2')";
        //PhoenixUtil.upsert(insertSql);
        String readByKeySql = "select * from TEST where mykey =2";
        //PhoenixUtil.readByKey(readByKeySql);

    }

}
