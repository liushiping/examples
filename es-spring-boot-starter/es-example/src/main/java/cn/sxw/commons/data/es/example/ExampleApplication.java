package cn.sxw.commons.data.es.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Created by William on 2018/8/7.
 */
@SpringBootApplication
@ComponentScan("cn.sxw.commons.data.es.example")
public class ExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExampleApplication.class, args);
    }
}
