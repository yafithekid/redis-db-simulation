package com.github.yafithekid.redisdbsimulation.service;

import com.github.yafithekid.redisdbsimulation.Quota;
import com.github.yafithekid.redisdbsimulation.repository.QuotaRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@Service
public class CachedPostgresService extends RequestCheckerService implements CacheScheduler {
    private List<Integer> applicationIds;
    private Map<Integer,Integer> quotas = new HashMap<>();
    private AtomicInteger jobFinished;
    protected AtomicInteger realWriteFinished = new AtomicInteger(0);
    protected long realWriteStart;
    private long startTime;

    @Value("${application.noperation}")
    private int noperation;

    @Value("${application.napp}")
    private int napp;

    private static Logger log = Logger.getLogger(CachedPostgresService.class.getName());
    private QuotaRepository quotaRepository;
    private Executor executor;

    @Autowired
    public CachedPostgresService(
            QuotaRepository quotaRepository,
            Executor executor
    ){
        this.quotaRepository = quotaRepository;
        this.executor = executor;
        applicationIds = Collections.synchronizedList(new ArrayList<>());
    }

    @Scheduled(fixedRate = SCHEDULE_MILLIS)
    @Override
    public void fetchQuota(){
        long startRead = System.currentTimeMillis();
        for(int i = 0; i < napp; i++){
            Quota quota = quotaRepository.findById(i);
            quotas.put(i,quota.getNcount());
        }
        log.info("read job finished in "+(System.currentTimeMillis()-startRead));
    }

    @Scheduled(fixedRate = SCHEDULE_MILLIS)
    @Override
    public void pushQuota(){
        List<Integer> appIdCopys;
        synchronized (this){
            appIdCopys = new ArrayList<>(applicationIds);
            applicationIds.clear();
        }
        for(Integer appId: appIdCopys){
            if (realWriteStart == 0){
                realWriteStart = System.currentTimeMillis();
            }
            executor.execute(()->{
                quotaRepository.incrementQuota(appId);
//            log.info("increment "+appId);
                int counter = realWriteFinished.incrementAndGet();
                if (counter == noperationWrite){
                    log.info(counter+" all write job finishes in "+(System.currentTimeMillis() - realWriteStart));
                    realWriteStart = 0;
                    realWriteFinished.set(0);
                } else if (counter % 500 == 0){
                    log.info(counter +" job finished");
                }
            });
        }
    }

    @Override
    int getNCount(int appId) {
        int nCount = quotas.get(appId);
        int currentReadFinished = readFinished.incrementAndGet();
        if (currentReadFinished == noperation){
            log.info("read finished in "+(System.currentTimeMillis() - readStartTime));
        }
        return nCount;
    }

    @Override
    void incrementNCount(int appId) {
        synchronized (this){
            applicationIds.add(appId);
        }
        int currentWriteFinished = writeFinished.incrementAndGet();
        if (currentWriteFinished == noperationWrite){
            log.info("write finished in"+(System.currentTimeMillis() - writeStartTime));
        }
    }

    @Override
    void reset() {
        quotaRepository.resetQuota();
    }

    void setStartTime(long startTime){
        this.startTime =startTime;
    }

    @Override
    void initialize() {
        super.initialize();
    }

}
