package com.github.yafithekid.redisdbsimulation.service;

import com.github.yafithekid.redisdbsimulation.Quota;
import com.github.yafithekid.redisdbsimulation.repository.QuotaRepository;
import com.github.yafithekid.redisdbsimulation.repository.RQuotaRepository;
import com.github.yafithekid.redisdbsimulation.repository.RequestLogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.*;
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
    private RedisTemplate redisTemplate;
    private Executor executor;
    private RequestLogRepository requestLogRepository;
    private IncrementBufferService incrementBufferService;
    private CachedRedisBufferService cachedRedisBufferService;
    private CachedPostgresService cachedPostgresService;
    private final RedisDirectService redisDirectService;
    private PostgresDirectService postgresDirectService;
    private final AtomicInteger readFinished;
    private static final Logger log = Logger.getLogger(RequestSimulationService.class.getName());

    @Value("${application.napp}")
    private int napp;

    @Value("${application.noperation}")
    private int noperation;

    @Value("${application.quotalimit}")
    private int nlimit;

    String quotaRedisKey = "quotas:";
    String quotaLogKey = "log:";
    private AtomicInteger jobFinished;
    private long start;

    @Autowired
    public RequestSimulationService(
            QuotaRepository quotaRepository,
            RQuotaRepository rQuotaRepository,
            StringRedisTemplate stringRedisTemplate,
            RedisTemplate redisTemplate,
            Executor executor,
            RequestLogRepository requestLogRepository,
            IncrementBufferService incrementBufferService,
            CachedRedisBufferService cachedRedisBufferService,
            CachedPostgresService cachedPostgresService,
            RedisDirectService redisDirectService,
            PostgresDirectService postgresDirectService,
            AtomicInteger readFinished
    ){

        this.quotaRepository = quotaRepository;
        this.rQuotaRepository = rQuotaRepository;
        this.stringRedisTemplate = stringRedisTemplate;
        this.redisTemplate = redisTemplate;
        this.executor = executor;
        this.requestLogRepository = requestLogRepository;
        this.incrementBufferService = incrementBufferService;
        this.cachedRedisBufferService = cachedRedisBufferService;
        this.cachedPostgresService = cachedPostgresService;
        this.redisDirectService = redisDirectService;
        this.postgresDirectService = postgresDirectService;
        this.readFinished = readFinished;
    }

    public void benchmark(BenchmarkType benchmarkType){
        ThreadPoolExecutor executor = (ThreadPoolExecutor) this.executor;
        RequestCheckerService requestCheckerService;
        if (benchmarkType == BenchmarkType.REDIS_CACHED){
            requestCheckerService = cachedRedisBufferService;
        } else if (benchmarkType == BenchmarkType.REDIS_DIRECT){
            requestCheckerService = redisDirectService;
        } else if (benchmarkType == BenchmarkType.POSTGRES){
            requestCheckerService = postgresDirectService;
        } else if (benchmarkType == BenchmarkType.POSTGRESQL_CACHED) {
            requestCheckerService = cachedPostgresService;
        } else {
            throw new IllegalStateException();
        }
        requestCheckerService.initialize();
        for(int i = 0; i < noperation; i++){
            int x = getAppId();
            executor.execute(() -> requestCheckerService.performRequest(x));
        }
    }

    public void benchmarkRedis(int version){
        resetRedis();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) this.executor;
        jobFinished.set(0);
        incrementBufferService.setJobFinishedCounter(jobFinished);
        start = System.currentTimeMillis();
        incrementBufferService.setStartTime(start);
        incrementBufferService.setRunJob(true);
        for(int i = 0; i < noperation; i++){
            int x = getAppId();
            if (version == 1){
                executor.execute(() -> executeRedis(""+ x));
            } else {
                executor.execute(() -> executeRedisV2(x));
            }
        }
    }

    public void benchmarkPostgres(){
        quotaRepository.resetQuota();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) this.executor;
        jobFinished.set(0);
        start = System.currentTimeMillis();
        for(int i = 0; i < noperation; i++){
            int x = getAppId();
            executor.execute(() -> executePsql(x));
        }
    }

    public void benchmarkPostgresV2(){
        quotaRepository.resetQuota();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) this.executor;
        jobFinished.set(0);
        start = System.currentTimeMillis();
        cachedPostgresService.setStartTime(start);
        for(int i = 0; i < noperation; i++){
            int x = getAppId();
            executor.execute(() -> cachedPostgresService.performRequest(x));
        }
    }

    public void executeRedisV2(int applicationId){
        incrementBufferService.add(applicationId);
    }

    public void executeRedis(String applicationId){
        log.info(""+System.currentTimeMillis());
        ValueOperations<String, String> valueOperations = stringRedisTemplate.opsForValue();
        ListOperations<String,String> listOperations = redisTemplate.opsForList();
        int i = Integer.parseInt(valueOperations.get(quotaRedisKey+applicationId));
        log.info(""+System.currentTimeMillis());
        if (i > nlimit){
            log.info("[redis] app "+applicationId+" is overquota");
        } else {
            valueOperations.increment(quotaRedisKey(applicationId),1);
            listOperations.rightPush(quotaLogKey(applicationId),"hello");
            log.info("[redis] app "+applicationId+" api call");
        }
        int i1 = jobFinished.incrementAndGet();
        if (i1 == noperation){
            log.info("finished in "+(System.currentTimeMillis() - start));
        }
    }

    public void executePsql(int applicationId){
        log.info(System.currentTimeMillis()+"");
        Quota byId = quotaRepository.findById(applicationId);
        log.info(System.currentTimeMillis()+"");
        if (byId.getNcount() > nlimit){
            log.info("[postgres] app "+applicationId+" is overquota");
        } else {
            quotaRepository.incrementQuota(applicationId);
            log.info("[postgres] app "+applicationId+" api call");
        }
//        log.info(System.currentTimeMillis()+"");
        int i1 = jobFinished.incrementAndGet();
        if (i1 == noperation){
            log.info("finished in "+(System.currentTimeMillis() - start));
        }
    }

    public void resetRedis(){
        ValueOperations<String,String> valueOperations = stringRedisTemplate.opsForValue();
        for(int i = 0; i < napp; i++){
            valueOperations.set(quotaRedisKey(i),"0");
            log.info("set redis app_id="+i+" to "+0);
        }
    }

    public void resetPgsql(){
        for(int i = 0; i < napp; i++){
            int finalI = i;
            Quota quota = quotaRepository.findOne(finalI);
            if (quota == null){
                quota = new Quota();
                quota.setId(finalI);
            }
            quota.setNcount(0);
            quotaRepository.save(quota);
            log.info("[postgres] set pgsql app_id="+ finalI +" to "+0);
        }
    }

    static int counter = -1;

    public int getAppId(){
        counter = (counter + 1) % napp;
        return counter;
    }

    String quotaRedisKey(int appId){
        return quotaRedisKey(appId+"");
    }

    String quotaRedisKey(String appId){
        return quotaRedisKey+appId;
    }

    String quotaLogKey(String appId){
        return quotaLogKey+appId;
    }
}
