package com.canal.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ImpalaApplication {

    public static void main(String[] args) {
        try {
            SpringApplication.run(ImpalaApplication.class, args);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
