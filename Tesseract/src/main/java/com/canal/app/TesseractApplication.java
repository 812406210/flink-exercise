package com.canal.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;

@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class })
public class TesseractApplication {

    public static void main(String[] args) {
        try {
            SpringApplication.run(TesseractApplication.class, args);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
