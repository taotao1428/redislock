package com.hewutao.redislock.util;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.types.Expiration;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class RedisLockUtil {
    /**
     * 获取一个Redis分布锁。会一直尝试，直到获得锁
     * @param connection redis连接
     * @param lockName 锁的名称
     * @param expireTime 锁的自动过期时间
     * @param expireTimeUnit 自动过期时间单位
     * @return 锁的identifier，用于释放锁
     */
    public static String lock(RedisConnection connection, String lockName,
                              long expireTime, TimeUnit expireTimeUnit) {
        String identifier = UUID.randomUUID().toString();
        String lockKey = getLockKey(lockName);
        Random rand = new Random();
        while (true) {
            if (setNX(connection, lockKey, identifier, expireTime, expireTimeUnit)) {
                return identifier;
            }
            // 等待50到100毫秒再尝试
            LockSupport.parkNanos((50 + rand.nextInt(50)) * 1000000);
        }
    }

    /**
     * 尝试获得锁，不会中断
     * @param connection redis连接
     * @param lockName 锁的名称
     * @param expireTime 锁自动过期时间
     * @param expireTimeUnit 过期时间单位
     * @return 如果获取成功返回identifier，失败返回null
     */
    public static String tryLock(RedisConnection connection, String lockName,
                                 long expireTime, TimeUnit expireTimeUnit) {
        String identifier = UUID.randomUUID().toString();
        String lockKey = getLockKey(lockName);
        if (setNX(connection, lockKey, identifier, expireTime, expireTimeUnit)) {
            return identifier;
        }
        return null;
    }

    /**
     * 获取锁，会尝试等待指定的时间
     * @param connection redis连接
     * @param lockName 锁的名称
     * @param time 等待的时间
     * @param timeUnit 等待时间单位
     * @param expireTime 锁自动过期时间
     * @param expireTimeUnit 自动过期时间单位
     * @return 如果在等待时间获得锁，返回identifier，否者返回null
     */
    public static String tryLock(RedisConnection connection,
                                 String lockName, long time, TimeUnit timeUnit,
                                 long expireTime, TimeUnit expireTimeUnit) {
        long end = System.nanoTime() + timeUnit.toNanos(time);
        Random rand = new Random();
        while (System.nanoTime() < end) {
            String identifier = tryLock(connection, lockName, expireTime, expireTimeUnit);
            if (identifier != null) {
                return identifier;
            }
            LockSupport.parkNanos((50 + rand.nextInt(50)) * 1000000);
        }
        return null;
    }

    /**
     * 释放redis锁
     * @param connection redis连接
     * @param lockName 锁的名称
     * @param identifier 锁的identifier，防止误删锁
     */
    public static void release(RedisConnection connection, String lockName, String identifier) {
        String lockKey = getLockKey(lockName);
        byte[] lockKeyBytes = lockKey.getBytes();
        connection.watch(lockKeyBytes);
        byte[] identifierInRedis;
        if ((identifierInRedis = connection.get(lockKeyBytes)) != null
                && identifier.equals(new String(identifierInRedis))) {
            connection.multi();
            connection.del(lockKeyBytes);
            connection.exec();
        }
    }

    private static boolean setNX(RedisConnection connection,
                                 String key, String value, long expireTime, TimeUnit timeUnit) {
        Boolean result = connection.set(key.getBytes(), value.getBytes(),
                Expiration.milliseconds(timeUnit.toMillis(expireTime)),  // 设置过期时间，放置死锁
                RedisStringCommands.SetOption.SET_IF_ABSENT);
        return result != null && result;
    }

    private static String getLockKey(String lockName) {
        return "lock:" + lockName;
    }
}

