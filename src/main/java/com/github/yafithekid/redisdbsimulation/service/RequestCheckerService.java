package com.github.yafithekid.redisdbsimulation.service;

import org.codehaus.janino.Java;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public abstract class RequestCheckerService {
    protected static final int SCHEDULE_MILLIS = 300;

    @Value("${application.napp}")
    protected int napp;

    @Value("${application.noperation}")
    protected int noperation;

    @Value("${application.quotalimit}")
    protected int nlimit;

    protected int noperationWrite;

    protected long readStartTime;

    protected long writeStartTime;

    protected AtomicInteger readFinished = new AtomicInteger(0);

    protected AtomicInteger writeFinished = new AtomicInteger(0);
    private static final Logger log  = Logger.getLogger(RequestCheckerService.class.getName());

    @Autowired
    public RequestCheckerService() {

    }

    @PostConstruct
    public void initNoperationWrite(){
        noperationWrite = Math.min(noperation,nlimit);
    }

    boolean performRequest(int appId){
        int nCount = getNCount(appId);
        if (nCount < nlimit){
            incrementNCount(appId);
            return true;
        } else {
//            log.info("over quota");
            return false;
        }
    }

    abstract int getNCount(int appId);

    abstract void incrementNCount(int appId);

    abstract void reset();

    void setReadStartTime(long readStartTime){
        this.readStartTime = readStartTime;
    }

    void setWriteStartTime(long writeStartTime){
        this.writeStartTime = writeStartTime;
    }

    String getRedisAppId(int appId){
        return "quotas:"+appId;
    }

    void initialize(){
        reset();
        setReadStartTime(System.currentTimeMillis());
        setWriteStartTime(System.currentTimeMillis());
        readFinished.set(0);
        writeFinished.set(0);
    }
}
