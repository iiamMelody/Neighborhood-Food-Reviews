package com.hmdp;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RedissonTest {
    @Autowired
    private RedissonClient redissonClient;

    @Autowired
    private RedissonClient redissonClient2;

    @Autowired
    private RedissonClient redissonClient3;

    private RLock lock;

    @BeforeEach
    void setUp(){
        RLock lock1 = redissonClient.getLock("order");
        RLock lock2 = redissonClient2.getLock("order");
        RLock lock3 = redissonClient3.getLock("order");

        //创建联锁 multiLock
        lock = redissonClient2.getMultiLock( lock1,lock2, lock3);
    }


    @Test
    void method1() throws InterruptedException {
        boolean isLock = lock.tryLock(1L, TimeUnit.SECONDS);
        if (!isLock){
            log.error("获取锁失败 ...1");
            return;
        }
        try {
            log.info("获取锁成功 ...1");
            method2();
            log.info("准备释放锁 ...1");
        }finally {
            log.warn("准备释放锁 ...1");
            lock.unlock();
        }
    }

    void method2(){
        //尝试获取锁
        boolean isLock = lock.tryLock();
        if (!isLock){
            log.error("获取锁失败 ...2");
            return;
        }
        try {
            log.info("获取锁成功 ...2");
            log.info("准备释放锁 ...2");
        }finally {
            log.warn("准备释放锁 ...2");
            lock.unlock();
        }

    }
}
