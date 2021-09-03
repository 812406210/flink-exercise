package com.ywj;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create:
 * 如果有druid的依赖，但是没有配置数据源，就要加上exclude = {DataSourceAutoConfiguration.class}
 */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class Neo4jApplication {

    public static void main(String[] args) {
        try {
            SpringApplication.run(Neo4jApplication.class, args);
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
