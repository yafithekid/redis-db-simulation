package com.github.yafithekid.redisdbsimulation;

import com.github.yafithekid.redisdbsimulation.repository.QuotaRepository;
import com.github.yafithekid.redisdbsimulation.service.BenchmarkType;
import com.github.yafithekid.redisdbsimulation.service.RequestSimulationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class TestController {
    private QuotaRepository quotaRepository;
    private StringRedisTemplate redisTemplate;

    @Value("${application.napp}")
    private int N_APP;
    private RequestSimulationService requestSimulationService;

    @Autowired
    public TestController(
            RequestSimulationService requestSimulationService,
            QuotaRepository quotaRepository,
            StringRedisTemplate redisTemplate
    ){
        this.requestSimulationService = requestSimulationService;
        this.quotaRepository = quotaRepository;
        this.redisTemplate = redisTemplate;
    }

    @GetMapping("/redis/test-direct")
    public String testRedis(){
        requestSimulationService.benchmark(BenchmarkType.REDIS_DIRECT);
        return "ok";
    }

    @GetMapping("/redis/test-indirect")
    public String testRedisV2(){
        requestSimulationService.benchmark(BenchmarkType.REDIS_CACHED);
        return "ok";
    }

    @GetMapping("/pgsql/test-direct")
    public String testPsql(){
        requestSimulationService.benchmark(BenchmarkType.POSTGRES);
        return "ok";
    }

    @GetMapping("/pgsql/test-indirect")
    public String testPsqlIndirect(){
        requestSimulationService.benchmark(BenchmarkType.POSTGRESQL_CACHED);
        return "ok";
    }

    @RequestMapping("/pgsql/insert")
    public String insertPostgres(){
        requestSimulationService.resetPgsql();
//        for(int i = 0; i < N_APP; i++){
//            Quota quota = quotaRepository.findOne(i);
//            if (quota == null){
//                quota = new Quota();
//                quota.setId(i);
//            }
//            quota.setNcount(0);
//            quotaRepository.save(quota);
//            System.out.println("set pgsql app_id="+i+" to "+0);
//        }
        return "";
    }

    @RequestMapping("/redis/insert")
    public String insertRedis(){
        ValueOperations<String,String> valueOperations = redisTemplate.opsForValue();
        for(int i = 0; i < N_APP; i++){
            valueOperations.set(i+"","0");
            System.out.println("set redis app_id="+i+" to "+0);
        }
        return "";
    }

    void run(){
        quotaRepository.incrementQuota(1);
    }

}
