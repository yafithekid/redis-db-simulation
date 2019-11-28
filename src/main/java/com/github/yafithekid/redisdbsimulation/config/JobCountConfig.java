package com.github.yafithekid.redisdbsimulation.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class JobCountConfig {
    @Bean
    public AtomicInteger readFinished(){
        return new AtomicInteger(0);
    }

    @Bean
    public AtomicInteger writeFinished(){
        return new AtomicInteger(0);
    }

}
