package com.github.yafithekid.redisdbsimulation.service;

import io.sentry.connection.RandomEventSampler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import sun.rmi.runtime.Log;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@Service
public class IncrementBufferService {
    private List<Integer> applicationIds;
    private Map<Integer,Integer> quotas = new HashMap<>();
    private StringRedisTemplate redisTemplate;
    private AtomicInteger jobFinished;
    private long startTime;
    private boolean runJob = false;

    @Value("${application.noperation}")
    private int noperation;

    @Value("${application.napp}")
    private int napp;

    private static Logger log = Logger.getLogger(IncrementBufferService.class.getName());

    @Autowired
    public IncrementBufferService(StringRedisTemplate redisTemplate){
        this.redisTemplate = redisTemplate;
        applicationIds = Collections.synchronizedList(new ArrayList<>());
    }

    @Scheduled(fixedRate = 5000)
    public void updateQuota(){
        ValueOperations<String, String> valueOperations = redisTemplate.opsForValue();

        for(int i = 0; i < napp; i++){
            String result = valueOperations.get("quotas:" + i);
            if (result == null){
                quotas.put(i,0);
            } else {
                quotas.put(i,Integer.parseInt(result));
            }
        }
        List<Object> objects = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            StringRedisConnection stringRedisConn = (StringRedisConnection) connection;
            for (int i = 0; i < napp; i++) {
                stringRedisConn.get("quotas:" + i);
            }
            return null;
        });
    }

    @Scheduled(fixedRate = 1000)
    public void run(){
        if (!runJob){
            return;
        }
        List<Integer> appIdCopys;
        synchronized (this){
            appIdCopys = new ArrayList<>(applicationIds);
            applicationIds.clear();
        }
        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            StringRedisConnection stringRedisConn = (StringRedisConnection)connection;
            for (Integer appId : appIdCopys) {
                stringRedisConn.incr("quotas:"+appId);
                jobFinished.incrementAndGet();
            }
            return null;
        });
        if (appIdCopys.size() > 0){
            log.info("consuming "+appIdCopys.size()+" jobs");
        }
        if (jobFinished.get() == noperation){
            log.info("finished in "+(System.currentTimeMillis() - startTime));
            jobFinished.set(0);
        }

    }

    public synchronized void add(int applicationId){
        applicationIds.add(applicationId);
    }

    public void setJobFinishedCounter(AtomicInteger atomicInteger){
        jobFinished = atomicInteger;
    }

    public void setStartTime(long startTime){
        this.startTime = startTime;
    }

    public void setRunJob(boolean runJob){
        this.runJob = runJob;
    }

}
