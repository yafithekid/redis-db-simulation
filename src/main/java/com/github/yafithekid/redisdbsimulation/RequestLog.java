package com.github.yafithekid.redisdbsimulation;

import org.springframework.context.annotation.Configuration;

import javax.persistence.*;

@Entity
@Table(name = "request_log")
public class RequestLog {
    @Column
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column
    private int applicationId;

    @Column
    private String target;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(int applicationId) {
        this.applicationId = applicationId;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }
}
