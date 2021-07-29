import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.sql.*;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-07-07 16:12
 */
public class Neo4jDemo {
    //https://neo4j.com/docs/cypher-manual/4.3/clauses/
    public static void main(String[] args) {
        try {
            Connection con = DriverManager.getConnection("jdbc:neo4j:bolt://localhost:7687/","neo4j","yang156122");

            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("match(p:Person {name: 'yang'}) return p");
            while(rs.next()) {
                //{"_id":0, "_labels":["Person"], "name":"Andres", "title":"Developer"}
                //System.out.println(rs.getString("p"));
                JSONObject dataJson = JSON.parseObject(rs.getString(1));
                System.out.println(dataJson);
            }
            } catch (SQLException e) {
                e.printStackTrace();
        }
        System.out.println("=========");
    }
}

/***
 * #创建
 * CREATE (n:Person { name: 'yang', title: 'yang-title' }) return n;
 * CREATE (n:Person { name: 'wen', title: 'wen-title' }) return n;
 * #创建关系
 * match (n:Person{name:'yang'}),(m:Person{name:'wen'}) create (n)<-[r:一家人]-(m) return r;
 * #查询关系
 * match (n:Person)-[r:`一家人`]-(p:Person) return distinct  n
 */
