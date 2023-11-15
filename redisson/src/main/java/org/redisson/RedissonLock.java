/**
 * Copyright (c) 2013-2022 Nikita Koksharov
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import io.netty.util.Timeout;
import org.redisson.api.RFuture;
import org.redisson.client.RedisTimeoutException;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.misc.CompletableFutureWrapper;
import org.redisson.pubsub.LockPubSub;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p>
 * Implements a <b>non-fair</b> locking so doesn't guarantees an acquire order.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonLock extends RedissonBaseLock {

    protected long internalLockLeaseTime;

    /**
     * 订阅
     */
    protected final LockPubSub pubSub;

    final CommandAsyncExecutor commandExecutor;

    public RedissonLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.internalLockLeaseTime = getServiceManager().getCfg().getLockWatchdogTimeout();
        this.pubSub = commandExecutor.getConnectionManager().getSubscribeService().getLockPubSub();
    }

    public RedissonLock(String name, CommandAsyncExecutor commandExecutor) {
        this(commandExecutor, name);
        this.name = name;
    }

    String getChannelName() {
        return prefixName("redisson_lock__channel", getRawName());
    }

    String getUnlockLatchName(String requestId) {
        return prefixName("redisson_unlock_latch", getRawName()) + ":" + requestId;
    }

    @Override
    public void lock() {
        try {
            lock(-1, null, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void lock(long leaseTime, TimeUnit unit) {
        try {
            lock(leaseTime, unit, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }


    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock(-1, null, true);
    }

    @Override
    public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
        lock(leaseTime, unit, true);
    }

    private void lock(long leaseTime, TimeUnit unit, boolean interruptibly) throws InterruptedException {
        long threadId = Thread.currentThread().getId();
        Long ttl = tryAcquire(-1, leaseTime, unit, threadId);
        // lock acquired
        if (ttl == null) {
            return;
        }

        CompletableFuture<RedissonLockEntry> future = subscribe(threadId);
        pubSub.timeout(future);
        RedissonLockEntry entry;
        if (interruptibly) {
            entry = commandExecutor.getInterrupted(future);
        } else {
            entry = commandExecutor.get(future);
        }

        try {
            while (true) {
                ttl = tryAcquire(-1, leaseTime, unit, threadId);
                // lock acquired
                if (ttl == null) {
                    break;
                }

                // waiting for message
                if (ttl >= 0) {
                    try {
                        entry.getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        if (interruptibly) {
                            throw e;
                        }
                        entry.getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                    }
                } else {
                    if (interruptibly) {
                        entry.getLatch().acquire();
                    } else {
                        entry.getLatch().acquireUninterruptibly();
                    }
                }
            }
        } finally {
            unsubscribe(entry, threadId);
        }
//        get(lockAsync(leaseTime, unit));
    }

    private Long tryAcquire(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        return get(tryAcquireAsync0(waitTime, leaseTime, unit, threadId));
    }

    private RFuture<Long> tryAcquireAsync0(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        return getServiceManager().execute(() -> tryAcquireAsync(waitTime, leaseTime, unit, threadId));
    }

    private RFuture<Boolean> tryAcquireOnceAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        // 结果值
        CompletionStage<Boolean> acquiredFuture;
        // 如果用户指定了续期时间 > 0
        if (leaseTime > 0) {
            acquiredFuture = tryLockInnerAsync(waitTime, leaseTime, unit, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        } else {
            // netty 发送脚本命令
            acquiredFuture = tryLockInnerAsync(waitTime,
                    internalLockLeaseTime, // 续期时间默认为 30 秒 org.redisson.config.Config.lockWatchdogTimeout
                    TimeUnit.MILLISECONDS, // 单位毫秒
                    threadId,               // 线程 id
                    RedisCommands.EVAL_NULL_BOOLEAN);
        }

        // 处理不同步
        acquiredFuture = handleNoSync(threadId, acquiredFuture);

        CompletionStage<Boolean> f = acquiredFuture.thenApply(acquired -> {
            // lock acquired
            // 如果 已获取锁
            if (acquired) {
                // 如果用户指定了续期时间
                if (leaseTime > 0) {
                    internalLockLeaseTime = unit.toMillis(leaseTime);
                } else {
                    // 创建定时续期任务
                    scheduleExpirationRenewal(threadId);
                }
            }
            // 返回是否获取到了锁
            return acquired;
        });
        return new CompletableFutureWrapper<>(f);
    }

    private RFuture<Long> tryAcquireAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        RFuture<Long> ttlRemainingFuture;
        if (leaseTime > 0) {
            ttlRemainingFuture = tryLockInnerAsync(waitTime, leaseTime, unit, threadId, RedisCommands.EVAL_LONG);
        } else {
            // 尝试获取锁
            ttlRemainingFuture = tryLockInnerAsync(waitTime, internalLockLeaseTime,
                    TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_LONG);
        }
        CompletionStage<Long> s = handleNoSync(threadId, ttlRemainingFuture);
        ttlRemainingFuture = new CompletableFutureWrapper<>(s);

        CompletionStage<Long> f = ttlRemainingFuture.thenApply(ttlRemaining -> {
            // lock acquired 已获取锁
            if (ttlRemaining == null) {
                if (leaseTime > 0) {
                    internalLockLeaseTime = unit.toMillis(leaseTime);
                } else {
                    scheduleExpirationRenewal(threadId);
                }
            }
            // 如果没有获取到锁,返回锁的存在时间
            return ttlRemaining;
        });
        return new CompletableFutureWrapper<>(f);
    }

    @Override
    public boolean tryLock() {
        // 尝试获取锁
        RFuture<Boolean> booleanRFuture = tryLockAsync();
        return get(booleanRFuture);
    }

    /**
     * exists  检查多个 key 是否存在。返回值 1：存在 0:不存在
     * <a href="https://redis.com.cn/commands/exists.html">...</a>
     * <p>
     * Hexists 查看哈希表的指定字段field 是否存在。 返回值：1 哈希表含有给定字段field。0 哈希表不含有给定字段，或 key 不存在。
     * <a href="https://redis.com.cn/commands/hexists.html">...</a>
     * <p>
     * hincrby 为存储在 key 中的哈希表指定字段做整数增量运算
     * PEXPIRE 设置 key 的过期时间，以毫秒计
     * PTTL	以毫秒为单位返回 key 的剩余的过期时间
     *
     * @return 如果上锁成功则返回 null, 上锁失败返回锁的剩余时间
     */
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        return evalWriteAsync(
                // String key
                getRawName(),
                // Codec codec
                LongCodec.INSTANCE,
                // RedisCommand<T> evalCommandTyp
                command,
                // String script
                "if ((redis.call('exists', KEYS[1]) == 0) " +           // 如果 key 不存在
                        "or (redis.call('hexists', KEYS[1], ARGV[2]) == 1)) then " + // 或者 hash 中的 线程 id 存在
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +    // 给map中的 key<线程id>，值自增1
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +   // 给 key 设置生命为 30 秒
                        "return nil; " +                                // 返回 null
                        "end; " +
                        "return redis.call('pttl', KEYS[1]);",          // 如果发现已被其它线程上锁了,则返回 key 剩余的时间
                // List<Object> keys
                Collections.singletonList(getRawName())    // keys[1]
                // Object... params
                , unit.toMillis(leaseTime),    // ARGV[1] 默认 30000 (30 秒)
                getLockName(threadId));        // ARGV[2] 线程 id
    }

    /**
     * 如果获取不到锁,则等待一定时间
     * @param waitTime the maximum time to acquire the lock
     * @param leaseTime lease time
     * @param unit time unit
     */
    @Override
    public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
        // 等待时间的毫秒数
        long time = unit.toMillis(waitTime);
        long current = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        // 尝试获取锁
        Long ttl = tryAcquire(waitTime, leaseTime, unit, threadId);
        // lock acquired ttl = null 表示获取到锁了, 直接返回
        if (ttl == null) {
            return true;
        }

        // System.currentTimeMillis() - current 的意思是(当前时间 - 第一次获取锁时间) = 请求的时间
        time -= System.currentTimeMillis() - current;
        // 如果请求获取锁的时间大于了指定获取锁的等待时间,代表获取锁失败了
        if (time <= 0) {
            acquireFailed(waitTime, unit, threadId);
            return false;
        }

        // 重置当前时间
        current = System.currentTimeMillis();
        // 订阅该分布式锁的 channel
        // 相当于等待解锁人告诉其它线程,锁被解了
        // Redis 发布订阅参考教程 https://www.runoob.com/redis/redis-pub-sub.html
        CompletableFuture<RedissonLockEntry> subscribeFuture = subscribe(threadId);
        try {
            subscribeFuture.get(time, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // 等待超时,获取锁失败
            if (!subscribeFuture.completeExceptionally(new RedisTimeoutException(
                    "Unable to acquire subscription lock after " + time + "ms. " +
                            "Try to increase 'subscriptionsPerConnection' and/or 'subscriptionConnectionPoolSize' parameters."))) {
                subscribeFuture.whenComplete((res, ex) -> {
                    if (ex == null) {
                        unsubscribe(res, threadId);
                    }
                });
            }
            // 获取锁失败
            acquireFailed(waitTime, unit, threadId);
            return false;
        } catch (ExecutionException e) {
            acquireFailed(waitTime, unit, threadId);
            return false;
        }

        try {
            // 计算第二次等待时间是否超时了
            time -= System.currentTimeMillis() - current;
            if (time <= 0) {
                acquireFailed(waitTime, unit, threadId);
                return false;
            }

            while (true) {
                long currentTime = System.currentTimeMillis();
                // 尝试争抢获取锁
                ttl = tryAcquire(waitTime, leaseTime, unit, threadId);
                // lock acquired 获取成功
                if (ttl == null) {
                    return true;
                }

                // 判断是否超时
                time -= System.currentTimeMillis() - currentTime;
                if (time <= 0) {
                    // 上锁失败
                    acquireFailed(waitTime, unit, threadId);
                    return false;
                }

                // waiting for message 等待消息
                currentTime = System.currentTimeMillis();
                if (ttl >= 0 && ttl < time) {
                    // 继续从订阅中等待获取解锁的消息
                    commandExecutor.getNow(subscribeFuture).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    commandExecutor.getNow(subscribeFuture).getLatch().tryAcquire(time, TimeUnit.MILLISECONDS);
                }

                time -= System.currentTimeMillis() - currentTime;
                if (time <= 0) {
                    acquireFailed(waitTime, unit, threadId);
                    return false;
                }
            }
        } finally {
            // 取消订阅
            unsubscribe(commandExecutor.getNow(subscribeFuture), threadId);
        }
//        return get(tryLockAsync(waitTime, leaseTime, unit));
    }

    protected CompletableFuture<RedissonLockEntry> subscribe(long threadId) {
        return pubSub.subscribe(getEntryName(), getChannelName());
    }

    protected void unsubscribe(RedissonLockEntry entry, long threadId) {
        pubSub.unsubscribe(entry, getEntryName(), getChannelName());
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        return tryLock(waitTime, -1, unit);
    }

    @Override
    protected void cancelExpirationRenewal(Long threadId) {
        super.cancelExpirationRenewal(threadId);
        this.internalLockLeaseTime = getServiceManager().getCfg().getLockWatchdogTimeout();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return commandExecutor.syncedEvalWithRetry(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('del', KEYS[1]) == 1) then "
                        + "redis.call(ARGV[2], KEYS[2], ARGV[1]); "
                        + "return 1 "
                        + "else "
                        + "return 0 "
                        + "end",
                Arrays.asList(getRawName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE, getSubscribeService().getPublishCommand());
    }

    protected RFuture<Boolean> unlockInnerAsync(long threadId, String requestId, int timeout) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local val = redis.call('get', KEYS[3]); " +
                        "if val ~= false then " +
                            "return tonumber(val);" +
                        "end; " +

                        "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                            "return nil;" +
                            "end; " +
                            "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        "if (counter > 0) then " +
                            "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                            "redis.call('set', KEYS[3], 0, 'px', ARGV[5]); " +
                            "return 0; " +
                        "else " +
                            "redis.call('del', KEYS[1]); " +
                            "redis.call(ARGV[4], KEYS[2], ARGV[1]); " +
                            "redis.call('set', KEYS[3], 1, 'px', ARGV[5]); " +
                        "return 1; " +
                        "end; ",
                Arrays.asList(getRawName(),                     // KEYS[1]
                        getChannelName(),                       // KEYS[2]
                        getUnlockLatchName(requestId)),         // KEYS[3]
                // param
                LockPubSub.UNLOCK_MESSAGE,                      // ARGV[1]
                internalLockLeaseTime,                          // ARGV[2]
                getLockName(threadId),                          // ARGV[3]
                getSubscribeService().getPublishCommand(),      // ARGV[4]
                timeout);                                       // ARGV[5]
    }

    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit, long currentThreadId) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        RFuture<Long> ttlFuture = tryAcquireAsync0(-1, leaseTime, unit, currentThreadId);
        ttlFuture.whenComplete((ttl, e) -> {
            if (e != null) {
                result.completeExceptionally(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                if (!result.complete(null)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            CompletableFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            pubSub.timeout(subscribeFuture);
            subscribeFuture.whenComplete((res, ex) -> {
                if (ex != null) {
                    result.completeExceptionally(ex);
                    return;
                }

                lockAsync(leaseTime, unit, res, result, currentThreadId);
            });
        });

        return new CompletableFutureWrapper<>(result);
    }

    private void lockAsync(long leaseTime, TimeUnit unit,
                           RedissonLockEntry entry, CompletableFuture<Void> result, long currentThreadId) {
        RFuture<Long> ttlFuture = tryAcquireAsync0(-1, leaseTime, unit, currentThreadId);
        ttlFuture.whenComplete((ttl, e) -> {
            if (e != null) {
                unsubscribe(entry, currentThreadId);
                result.completeExceptionally(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                unsubscribe(entry, currentThreadId);
                if (!result.complete(null)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            if (entry.getLatch().tryAcquire()) {
                lockAsync(leaseTime, unit, entry, result, currentThreadId);
            } else {
                // waiting for message
                AtomicReference<Timeout> futureRef = new AtomicReference<>();
                Runnable listener = () -> {
                    if (futureRef.get() != null) {
                        futureRef.get().cancel();
                    }
                    lockAsync(leaseTime, unit, entry, result, currentThreadId);
                };
                entry.addListener(listener);

                if (ttl >= 0) {
                    Timeout scheduledFuture = getServiceManager().newTimeout(timeout -> {
                        if (entry.removeListener(listener)) {
                            lockAsync(leaseTime, unit, entry, result, currentThreadId);
                        }
                    }, ttl, TimeUnit.MILLISECONDS);
                    futureRef.set(scheduledFuture);
                }
            }
        });
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long threadId) {
        // 异步执行, 返回 RFuture
        return getServiceManager().execute(() -> tryAcquireOnceAsync(-1, -1, null, threadId));
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit,
                                         long currentThreadId) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        AtomicLong time = new AtomicLong(unit.toMillis(waitTime));
        long currentTime = System.currentTimeMillis();
        RFuture<Long> ttlFuture = tryAcquireAsync0(waitTime, leaseTime, unit, currentThreadId);
        ttlFuture.whenComplete((ttl, e) -> {
            if (e != null) {
                result.completeExceptionally(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                if (!result.complete(true)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            long el = System.currentTimeMillis() - currentTime;
            time.addAndGet(-el);

            if (time.get() <= 0) {
                trySuccessFalse(currentThreadId, result);
                return;
            }

            long current = System.currentTimeMillis();
            AtomicReference<Timeout> futureRef = new AtomicReference<>();
            CompletableFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            pubSub.timeout(subscribeFuture, time.get());
            subscribeFuture.whenComplete((r, ex) -> {
                if (ex != null) {
                    result.completeExceptionally(ex);
                    return;
                }

                if (futureRef.get() != null) {
                    futureRef.get().cancel();
                }

                long elapsed = System.currentTimeMillis() - current;
                time.addAndGet(-elapsed);

                tryLockAsync(time, waitTime, leaseTime, unit, r, result, currentThreadId);
            });
            if (!subscribeFuture.isDone()) {
                Timeout scheduledFuture = getServiceManager().newTimeout(timeout -> {
                    if (!subscribeFuture.isDone()) {
                        subscribeFuture.cancel(false);
                        trySuccessFalse(currentThreadId, result);
                    }
                }, time.get(), TimeUnit.MILLISECONDS);
                futureRef.set(scheduledFuture);
            }
        });


        return new CompletableFutureWrapper<>(result);
    }

    private void tryLockAsync(AtomicLong time, long waitTime, long leaseTime, TimeUnit unit,
                              RedissonLockEntry entry, CompletableFuture<Boolean> result, long currentThreadId) {
        if (result.isDone()) {
            // 取消订阅
            unsubscribe(entry, currentThreadId);
            return;
        }

        if (time.get() <= 0) {
            unsubscribe(entry, currentThreadId);
            trySuccessFalse(currentThreadId, result);
            return;
        }

        long curr = System.currentTimeMillis();
        RFuture<Long> ttlFuture = tryAcquireAsync0(waitTime, leaseTime, unit, currentThreadId);
        ttlFuture.whenComplete((ttl, e) -> {
            if (e != null) {
                unsubscribe(entry, currentThreadId);
                result.completeExceptionally(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                unsubscribe(entry, currentThreadId);
                if (!result.complete(true)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            long el = System.currentTimeMillis() - curr;
            time.addAndGet(-el);

            if (time.get() <= 0) {
                unsubscribe(entry, currentThreadId);
                trySuccessFalse(currentThreadId, result);
                return;
            }

            // waiting for message
            long current = System.currentTimeMillis();
            if (entry.getLatch().tryAcquire()) {
                tryLockAsync(time, waitTime, leaseTime, unit, entry, result, currentThreadId);
            } else {
                AtomicBoolean executed = new AtomicBoolean();
                AtomicReference<Timeout> futureRef = new AtomicReference<>();
                Runnable listener = () -> {
                    executed.set(true);
                    if (futureRef.get() != null) {
                        // 取消续期任务
                        futureRef.get().cancel();
                    }
                    long elapsed = System.currentTimeMillis() - current;
                    time.addAndGet(-elapsed);

                    tryLockAsync(time, waitTime, leaseTime, unit, entry, result, currentThreadId);
                };
                entry.addListener(listener);

                long t = time.get();
                if (ttl >= 0 && ttl < time.get()) {
                    t = ttl;
                }
                if (!executed.get()) {
                    Timeout scheduledFuture = getServiceManager().newTimeout(timeout -> {
                        if (entry.removeListener(listener)) {
                            long elapsed = System.currentTimeMillis() - current;
                            time.addAndGet(-elapsed);

                            tryLockAsync(time, waitTime, leaseTime, unit, entry, result, currentThreadId);
                        }
                    }, t, TimeUnit.MILLISECONDS);
                    futureRef.set(scheduledFuture);
                }
            }
        });
    }


}
