package com.ywj;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.ywj.service.*")
public class EsApplication {

    public static void main(String[] args) {
        try {
            SpringApplication.run(EsApplication.class, args);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
