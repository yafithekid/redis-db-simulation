package com.github.yafithekid.redisdbsimulation.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

@Service
public class RedisDirectService extends RequestCheckerService {
    private static Logger log = Logger.getLogger(RedisDirectService.class.getName());
    private final RedisTemplate<String,String> stringRedisTemplate;

    @Autowired
    public RedisDirectService(
            RedisTemplate<String,String> stringRedisTemplate
    ){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    int getNCount(int appId) {
        ValueOperations<String, String> valueOperations = stringRedisTemplate.opsForValue();
        String result = valueOperations.get(getRedisAppId(appId));
        int i = readFinished.incrementAndGet();
//        log.info("get "+appId);
//        log.info("read total = "+i);
        if (i == noperation){
            log.info(i+" read finished in "+(System.currentTimeMillis() - readStartTime));
        } else if (i % 500 == 0){
            log.info("done " +i + " reads");
        }
        return Integer.parseInt(result);
    }

    @Override
    void incrementNCount(int appId) {
        ValueOperations<String, String> valueOperations = stringRedisTemplate.opsForValue();
        valueOperations.increment(getRedisAppId(appId),1);
        int i = writeFinished.incrementAndGet();
//        log.info("write "+appId);
//        log.info("write total = "+i);
        if (i == noperationWrite){
            log.info(i +" write finished in "+(System.currentTimeMillis() - writeStartTime));
        } else if (i % 500 == 0){
            log.info("done " +i + " writes");
        }
    }

    @Override
    void reset() {
        ValueOperations<String,String> valueOperations = stringRedisTemplate.opsForValue();
        for(int i = 0; i < napp; i++){
            valueOperations.set(getRedisAppId(i),"0");
        }
        readFinished.set(0);
        writeFinished.set(0);
    }
}
