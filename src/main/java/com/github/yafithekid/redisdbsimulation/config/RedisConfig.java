package com.github.yafithekid.redisdbsimulation.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisSentinelCommands;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
@EnableRedisRepositories

public class RedisConfig {
    private RedisProperties redisProperties;

    @Autowired
    public RedisConfig(RedisProperties redisProperties){
        this.redisProperties = redisProperties;
    }

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory();
        RedisProperties redisProperties = this.redisProperties;
        jedisConnectionFactory.setHostName(redisProperties.getHost());
        jedisConnectionFactory.setDatabase(redisProperties.getDatabase());
        jedisConnectionFactory.setTimeout(redisProperties.getTimeout());
        return jedisConnectionFactory;
    }

    @Bean
    public StringRedisTemplate stringRedisTemplate() {
        StringRedisTemplate stringRedisTemplate = new StringRedisTemplate(redisConnectionFactory());
        return stringRedisTemplate;
    }

    @Bean
    public RedisTemplate<String,String> redisTemplate() {
        RedisTemplate<String,String> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(redisConnectionFactory());
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new StringRedisSerializer());
        return redisTemplate;
    }
}
