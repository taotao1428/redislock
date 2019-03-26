package com.hewutao.redislock;

import com.hewutao.redislock.util.RedisLockUtil;
import org.springframework.data.redis.connection.RedisConnection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

/**
 * redis的重入锁
 * 机制：使用aqs管理需要获得锁的线程，仅有获得本地锁的线程该可以去请求redis锁
 * 这样可以减少对redis的锁请求
 */
public class RedisReentrantLock implements Lock {
    // redis的连接
    private final RedisConnection conn;

    private final String lockName; // 锁的名称
    private volatile String identifier; // 锁的标识符
    private final long expireTime; // 锁自动过期时间，这个需要根据具体场景确定时间
    private final TimeUnit expireTimeUnit; // 自动过期时间单位

    private Sync sync = new Sync();

    public RedisReentrantLock(RedisConnection conn, String lockName, long expireTime, TimeUnit expireTimeUnit) {
        if (lockName == null || conn == null) {
            throw new IllegalArgumentException();
        }
        this.lockName = lockName;
        this.conn = conn;
        this.expireTime = expireTime;
        this.expireTimeUnit = expireTimeUnit;
    }

    class Sync extends AbstractQueuedSynchronizer {
        @Override
        protected boolean tryAcquire(int acquires) {
            int state = getState();
            final Thread t = Thread.currentThread();
            if (state == 0) { // 先获得线程锁
                if (compareAndSetState(0, acquires)) {
                    setExclusiveOwnerThread(t);
                    try {
                        redisLock();
                    } catch (Exception e) {
                        redisLockFail(t, acquires);
                        throw e;
                    }
                    return true;
                }
            } else if (getExclusiveOwnerThread() == t) {
                int nextc = state + acquires;
                if (nextc < 0) {
                    throw new Error("maximum lock count exceeded");
                }
                setState(nextc);
                return true;
            }
            return false; // 如果当前线程已经拥有锁
        }

        private boolean tryAcquireNonBlocking(int acquires) {
            int state = getState();
            Thread t = Thread.currentThread();
            if (state == 0) { // 先获得线程锁
                if (compareAndSetState(0, acquires)) {
                    setExclusiveOwnerThread(t);
                    try {
                        if (redisTryLock()) { // 尝试获得redis锁
                            return true;
                        }
                    } catch (Exception e) {
                        redisLockFail(t, acquires);
                        throw e;
                    }
                    release(acquires);
                }
                return false;
            } else if (getExclusiveOwnerThread() == t) {
                int nextc = state + acquires;
                if (nextc < 0) {
                    throw new Error("maximum lock count exceeded");
                }
                setState(nextc);
                return true;
            }
            return false; // 如果当前线程已经拥有锁
        }

        /**
         * 当尝试获取redis锁出现异常时
         * @param t 当前线程
         * @param acquires 线程获得锁
         */
        private void redisLockFail(Thread t, int acquires) {
            release(acquires); // 调用release
        }


        @Override
        protected boolean tryRelease(int acquires) {
            if (getExclusiveOwnerThread() != Thread.currentThread()) {
                throw new IllegalMonitorStateException("当前线程还没有拥有锁");
            }

            int s = getState() - acquires;
            boolean free = false;
            if (s == 0) {
                free = true;
                redisRelease(); // 如果当前线程释放了锁，也就释放了redis锁
                setExclusiveOwnerThread(null);
            }

            setState(s);
            return free;
        }
    }

    /**
     * 获得redis锁，会一直阻塞线程，无法中断
     */
    private void redisLock() {
        this.identifier = RedisLockUtil.lock(conn, lockName, expireTime, expireTimeUnit);
    }

    /**
     * 尝试获得redis锁
     * @return 获得成功返回true，否者返回false
     */
    private boolean redisTryLock() {
        String identifier = RedisLockUtil.tryLock(conn, lockName, expireTime, expireTimeUnit);
        if (identifier != null) {
            this.identifier = identifier;
            return true;
        }
        return false;
    }

    /**
     * 释放redis锁
     */
    private void redisRelease() {
        if (identifier != null) {
            try {
                RedisLockUtil.release(conn, lockName, identifier);
            } catch (Exception e) {
                e.printStackTrace();
            }
            identifier = null;
        }
    }


    @Override
    public void lock() {
        sync.acquire(1);
    }

    @Override
    public boolean tryLock() {
        return sync.tryAcquireNonBlocking(1);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return sync.tryAcquireNanos(1, unit.toNanos(time));
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        sync.acquireInterruptibly(1);
    }

    @Override
    public void unlock() {
        sync.release(1);
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}

