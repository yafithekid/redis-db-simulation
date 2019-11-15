package com.github.yafithekid.redisdbsimulation;

import org.springframework.data.redis.core.RedisHash;

import javax.persistence.Id;

@RedisHash("rquota")
public class RQuota {
    @Id
    String id;
    int quota;

}
