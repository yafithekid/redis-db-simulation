package com.github.yafithekid.redisdbsimulation.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Configuration
public class ThreadPoolConfig {
    @Value("${application.threadpoolsize}")
    private int threadPoolSize;

    @Bean
    public Executor executor(){
        return Executors.newFixedThreadPool(threadPoolSize);
    }
}
