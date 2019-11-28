package com.github.yafithekid.redisdbsimulation.repository;

import com.github.yafithekid.redisdbsimulation.RequestLog;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RequestLogRepository extends JpaRepository<RequestLog,Integer> {
}
