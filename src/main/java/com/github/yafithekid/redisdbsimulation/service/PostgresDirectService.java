package com.github.yafithekid.redisdbsimulation.service;

import com.github.yafithekid.redisdbsimulation.Quota;
import com.github.yafithekid.redisdbsimulation.repository.QuotaRepository;
import org.codehaus.janino.Java;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import sun.rmi.runtime.Log;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@Service
public class PostgresDirectService extends RequestCheckerService {
    private QuotaRepository quotaRepository;
    private static Logger log = Logger.getLogger(PostgresDirectService.class.getName());

    @Autowired
    public PostgresDirectService(
            QuotaRepository quotaRepository
    ){
        this.quotaRepository = quotaRepository;
    }

    @Override
    int getNCount(int appId) {
        Quota byId = quotaRepository.findById(appId);
//        log.info("get "+appId);

        int i = readFinished.incrementAndGet();
        if (i == noperation){
            log.info("read finished in "+(System.currentTimeMillis() - readStartTime));
        } else if (i % 500 == 0){
            log.info("done " +i + " reads");
        }
        return byId.getNcount();
    }

    @Override
    void incrementNCount(int appId) {
        quotaRepository.incrementQuota(appId);
//        log.info("increment "+appId);
        int i = writeFinished.incrementAndGet();
        if (i == noperationWrite){
            log.info(i +" write finished in "+(System.currentTimeMillis() - writeStartTime));
        } else if (i % 500 == 0){
            log.info("done " +i + " writes");
        }
    }

    @Override
    void reset() {
        quotaRepository.resetQuota();
    }
}
