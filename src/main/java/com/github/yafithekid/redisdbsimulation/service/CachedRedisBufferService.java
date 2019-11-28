package com.github.yafithekid.redisdbsimulation.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@Service
public class CachedRedisBufferService extends RequestCheckerService implements CacheScheduler {
    private List<Integer> applicationIds;
    private Map<Integer,Integer> quotas = new HashMap<>();
    private StringRedisTemplate redisTemplate;
    protected long startRealWrite;
    protected AtomicInteger realWriteFinished = new AtomicInteger(0);

    private static Logger log = Logger.getLogger(CachedRedisBufferService.class.getName());

    @Autowired
    public CachedRedisBufferService(
            StringRedisTemplate redisTemplate
    ){
        this.redisTemplate = redisTemplate;
        applicationIds = Collections.synchronizedList(new ArrayList<>());
    }

    @Scheduled(fixedRate = SCHEDULE_MILLIS)
    @Override
    public void fetchQuota(){
        long startRealRead = System.currentTimeMillis();

        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            StringRedisConnection stringRedisConn = (StringRedisConnection) connection;
            for (int i = 0; i < napp; i++) {
                String result = stringRedisConn.get(getRedisAppId(i));
                if (result == null){
                    quotas.put(i,0);
                } else {
                    quotas.put(i,Integer.parseInt(result));
                }
            }
            log.info("read job finished in "+(System.currentTimeMillis() - startRealRead));
            return null;
        });
    }

    @Scheduled(fixedRate = SCHEDULE_MILLIS)
    @Override
    public void pushQuota(){
        List<Integer> appIdCopys;
        synchronized (this){
            appIdCopys = new ArrayList<>(applicationIds);
            applicationIds.clear();
        }
        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            StringRedisConnection stringRedisConn = (StringRedisConnection)connection;
            for (Integer appId : appIdCopys) {
                int currentRealWrite = realWriteFinished.get();
                if (currentRealWrite == 0){
                    startRealWrite = System.currentTimeMillis();
                }
                stringRedisConn.incr(getRedisAppId(appId));
//                log.info("write "+appId);
                currentRealWrite = realWriteFinished.incrementAndGet();
                if (currentRealWrite == noperationWrite){
                    log.info(currentRealWrite+ " write finished "+(System.currentTimeMillis() - startRealWrite));
                }
            }
            return null;
        });
    }

    @Override
    int getNCount(int appId) {
        Integer ret = quotas.get(appId);
        int currentReadFinished = readFinished.incrementAndGet();
        if (currentReadFinished == noperation){
            log.info(currentReadFinished+ " read finished in "+(System.currentTimeMillis() - readStartTime));
        } else if (currentReadFinished % 500 == 0){
            log.info("done " +currentReadFinished + " reads");
        }
        return ret;
    }

    @Override
    void incrementNCount(int appId) {
        synchronized (this){
            applicationIds.add(appId);
        }
        int i = writeFinished.incrementAndGet();
        if (i == noperationWrite){
            log.info(i+ " write finished in "+(System.currentTimeMillis() - startRealWrite));
        } else if (i % 500 == 0){
            log.info("done " +i + " writes");
        }
    }

    @Override
    void reset() {
        ValueOperations<String,String> valueOperations = redisTemplate.opsForValue();
        for(int i = 0; i < napp; i++){
            valueOperations.set(getRedisAppId(i),"0");
            log.info("set redis app_id="+i+" to "+0);
        }
    }

    @Override
    void initialize() {
        super.initialize();
        realWriteFinished.set(0);
        startRealWrite = System.currentTimeMillis();
    }
}
