package com.github.yafithekid.redisdbsimulation.service;

import com.github.yafithekid.redisdbsimulation.Quota;
import com.github.yafithekid.redisdbsimulation.repository.QuotaRepository;
import com.github.yafithekid.redisdbsimulation.repository.RQuotaRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@Service
public class RequestSimulationService {

    private final QuotaRepository quotaRepository;
    private final RQuotaRepository rQuotaRepository;
    private final StringRedisTemplate stringRedisTemplate;
    private Executor executor;
    private static final Logger log = Logger.getLogger(RequestSimulationService.class.getName());

    @Value("${application.napp}")
    private int napp;

    @Value("${application.noperation}")
    private int noperation;

    @Value("${application.quotalimit}")
    private int nlimit;

    private AtomicInteger finishedOperation = new AtomicInteger();
    private long start;

    public RequestSimulationService(
            QuotaRepository quotaRepository,
            RQuotaRepository rQuotaRepository,
            StringRedisTemplate stringRedisTemplate,
            Executor executor
    ){

        this.quotaRepository = quotaRepository;
        this.rQuotaRepository = rQuotaRepository;
        this.stringRedisTemplate = stringRedisTemplate;
        this.executor = executor;
    }

    public void benchmarkRedis(){
        resetRedis();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) this.executor;
        finishedOperation.set(0);
        start = System.currentTimeMillis();
        for(int i = 0; i < noperation; i++){
            int x = getAppId();
            executor.execute(() -> executeRedis(""+ x));
        }
    }

    public void benchmarkPostgres(){
        resetPgsql();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) this.executor;
        finishedOperation.set(0);
        start = System.currentTimeMillis();
        for(int i = 0; i < noperation; i++){
            int x = getAppId();
            executor.execute(() -> executePsql(x));
        }
    }

    public void executeRedis(String applicationId){
        ValueOperations<String, String> valueOperations = stringRedisTemplate.opsForValue();
        int i = Integer.parseInt(valueOperations.get(applicationId));
        if (i > nlimit){
            log.info("[redis] app "+applicationId+" is overquota");
        } else {
            valueOperations.increment(applicationId,1);
            log.info("[redis] app "+applicationId+" api call");
        }
        int i1 = finishedOperation.incrementAndGet();
        if (i1 == noperation){
            log.info("finished in "+(System.currentTimeMillis() - start));
        }
    }

    public void executePsql(int applicationId){
        if (!quotaRepository.existsByIdAndNcountLessThanEqual(applicationId, nlimit)){
            log.info("[postgres] app "+applicationId+" is overquota");
        } else {
            quotaRepository.incrementQuota(applicationId);
            log.info("[postgres] app "+applicationId+" api call");
        }
        int i1 = finishedOperation.incrementAndGet();
        if (i1 == noperation){
            log.info("finished in "+(System.currentTimeMillis() - start));
        }
    }

    public void resetRedis(){
        ValueOperations<String,String> valueOperations = stringRedisTemplate.opsForValue();
        for(int i = 0; i < napp; i++){
            valueOperations.set(i+"","0");
            log.info("set redis app_id="+i+" to "+0);
        }
    }

    public void resetPgsql(){
        for(int i = 0; i < napp; i++){
            Quota quota = quotaRepository.findOne(i);
            if (quota == null){
                quota = new Quota();
                quota.setId(i);
            }
            quota.setNcount(0);
            quotaRepository.save(quota);
            log.info("[postgres] set pgsql app_id="+i+" to "+0);
        }
    }

    static int counter = -1;

    public int getAppId(){
        counter = (counter + 1) % napp;
        return counter;
    }

}
