package com.pipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DataPipelineApplication {

    static {
        // Ensure local Spark/HDFS client uses superuser identity for HDFS operations
        System.setProperty("HADOOP_USER_NAME", "hadoop");
    }

    public static void main(String[] args) {
        SpringApplication.run(DataPipelineApplication.class, args);
    }
}
