package com.tuan.sl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Hello world!
 *
 */
//--spring.profiles.active=local
@SpringBootApplication
public class DataPipelineApp
{
    public static void main( String[] args ) {
        ConfigurableApplicationContext ca = SpringApplication.run(DataPipelineApp.class,args);
    }
}
