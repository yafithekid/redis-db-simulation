package com.github.yafithekid.redisdbsimulation.repository;

import com.github.yafithekid.redisdbsimulation.Quota;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface QuotaRepository extends JpaRepository<Quota,Integer> {
    @Modifying
    @Query(value = "UPDATE quotas SET ncount = ncount + 1 WHERE id=:id",nativeQuery = true)
    @Transactional
    void incrementQuota(@Param("id") int id);

    boolean existsByIdAndNcountLessThanEqual(int id,int ncount);

}
