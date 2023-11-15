/**
 * Copyright (c) 2013-2022 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.RedisException;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.convertor.IntegerReplayConvertor;
import org.redisson.client.protocol.decoder.MapValueDecoder;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandAsyncService;
import org.redisson.command.CommandBatchService;
import org.redisson.config.MasterSlaveServersConfig;
import org.redisson.misc.CompletableFutureWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.function.Supplier;

/**
 * Base class for implementing distributed locks
 *
 * @author Danila Varatyntsev
 * @author Nikita Koksharov
 */
public abstract class RedissonBaseLock extends RedissonExpirable implements RLock {

    /**
     * 续期任务封装
     */
    public static class ExpirationEntry {

        private final Map<Long/*线程 id*/, Integer/*计数器*/> threadIds = new LinkedHashMap<>();
        // netty 时间轮的延迟任务
        private volatile Timeout timeout;

        public ExpirationEntry() {
            super();
        }

        public synchronized void addThreadId(long threadId) {
            threadIds.compute(threadId, (t, counter) -> {
                counter = Optional.ofNullable(counter).orElse(0);
                counter++;
                return counter;
            });
        }
        public synchronized boolean hasNoThreads() {
            return threadIds.isEmpty();
        }
        public synchronized Long getFirstThreadId() {
            if (threadIds.isEmpty()) {
                return null;
            }
            return threadIds.keySet().iterator().next();
        }

        /*
        在 Java 中，Map 接口提供了一个 `compute` 方法，用于根据指定的键和计算函数来执行计算操作并更新 Map 中的映射关系。`compute` 方法的作用可以总结如下：
        1. 如果指定的键存在于 Map 中，并且对应的值不为 null，那么 `compute` 方法会使用指定的计算函数对原值进行计算，并将计算结果更新到 Map 中。
        2. 如果指定的键存在于 Map 中，但对应的值为 null，那么 `compute` 方法不会执行计算函数，也不会更新 Map 中的映射关系。
        3. 如果指定的键不存在于 Map 中，那么 `compute` 方法会将指定的键值对添加到 Map 中。

        `compute` 方法的声明如下：
            V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction)

        其中，`key` 是要进行计算的键，`remappingFunction` 是一个计算函数，它接受键和对应的值作为参数，并返回一个新的值。这个函数将被用于计算新的值。如果计算函数返回 null，则表示将该键从 Map 中删除。
        下面是一个简单的示例，演示了如何使用 `compute` 方法来更新 Map 中的映射关系：
            Map<String, Integer> map = new HashMap<>();
            map.put("A", 1);
            map.put("B", 2);

        // 对指定键进行计算操作
        map.compute("A", (k, v) -> (v == null) ? 42 : v * 2); // 对应的值不为 null，进行计算
        map.compute("C", (k, v) -> (v == null) ? 42 : v * 2); // 指定的键不存在，添加新的键值对
        System.out.println(map); // 输出：{A=2, B=2, C=42}
        ```
        在上面的示例中，我们首先创建了一个 HashMap，并向其中放入了两个键值对。然后，我们使用 `compute` 方法对键"A"和键"C"进行计算操作。通过指定的计算函数，我们更新了键"A"对应的值，以及向 Map 中添加了新的键值对"C"。
         */
        public synchronized void removeThreadId(long threadId) {
            threadIds.compute(threadId, (t, counter) -> {
                // 如果计数器 == null
                if (counter == null) {
                    return null; // 返回null
                }
                counter--; // 计数器自减
                if (counter == 0) { // 如果计数器 =0
                    return null; // 返回 null, 不做任何操作
                }
                return counter; // 返回计数器的值
            });
        }

        public void setTimeout(Timeout timeout) {
            this.timeout = timeout;
        }
        public Timeout getTimeout() {
            return timeout;
        }

    }

    private static final Logger log = LoggerFactory.getLogger(RedissonBaseLock.class);

    /**
     * 到期续订 map
     * 管理了所有的续期集合
     */
    private static final ConcurrentMap<String/*Redis锁的名称*/, ExpirationEntry/*延迟任务实例*/> EXPIRATION_RENEWAL_MAP = new ConcurrentHashMap<>();
    protected long internalLockLeaseTime;

    final String id;
    final String entryName;

    public RedissonBaseLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.id = getServiceManager().getId();
        this.internalLockLeaseTime = getServiceManager().getCfg().getLockWatchdogTimeout();
        this.entryName = id + ":" + name;
    }

    protected String getEntryName() {
        return entryName;
    }

    protected String getLockName(long threadId) {
        return id + ":" + threadId;
    }

    private void renewExpiration() {
        // 获取续期任务实例
        ExpirationEntry ee = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (ee == null) {
            return;
        }
        // 创建定时任务 10 秒后执行
        Timeout task = getServiceManager().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                // 每次执行续期任务之前,都判断一下 Map 中是否还存在这个任务
                // 如果解锁了,这里就获取不到了
                ExpirationEntry ent = EXPIRATION_RENEWAL_MAP.get(getEntryName());
                if (ent == null) {
                    return;
                }
                Long threadId = ent.getFirstThreadId();
                if (threadId == null) {
                    return;
                }
                // 执行锁续期脚本命令. 发送 netty 请求
                CompletionStage<Boolean> future = renewExpirationAsync(threadId);
                // 当续期任务执行完成以后
                future.whenComplete((res, e) -> {
                    // 如果出现异常
                    if (e != null) {
                        log.error("Can't update lock {} expiration", getRawName(), e);
                        EXPIRATION_RENEWAL_MAP.remove(getEntryName());
                        return;
                    }
                    // 如果需求成功 true
                    if (res) {
                        // 重新创建延迟任务继续续期
                        // reschedule itself
                        renewExpiration();
                    } else {
                        // 取消自动续期任务
                        cancelExpirationRenewal(null);
                    }
                });
            }
        }, internalLockLeaseTime / 3, TimeUnit.MILLISECONDS);   // 默认 10 秒后执行
        
        ee.setTimeout(task);
    }

    /**
     * 创建锁定时续期任务
     * @param threadId 线程 id
     */
    protected void scheduleExpirationRenewal(long threadId) {
        // 创建一个续期任务
        ExpirationEntry entry = new ExpirationEntry();
        // 如果不存在则向map中 PUT
        ExpirationEntry oldEntry = EXPIRATION_RENEWAL_MAP.putIfAbsent(getEntryName(), entry);
        // 如果旧的实例不存在
        if (oldEntry != null) {
            // 给续期任务添加线程 id
            oldEntry.addThreadId(threadId); // 给旧的实例添加线程 id
        } else {
            // 给续期任务添加线程 id
            entry.addThreadId(threadId);
            try {
                // Redis 锁续订到期
                renewExpiration();
            } finally {
                // 如果当前线程被中断
                if (Thread.currentThread().isInterrupted()) {
                    // 取消续期. 将续期任务实例从 EXPIRATION_RENEWAL_MAP 中移除
                    cancelExpirationRenewal(threadId);
                }
            }
        }
    }

    protected CompletionStage<Boolean> renewExpirationAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return 1; " +
                        "end; " +
                        "return 0;",
                Collections.singletonList(getRawName()),
                internalLockLeaseTime, getLockName(threadId));
    }

    protected void cancelExpirationRenewal(Long threadId) {
        // 尝试从 Map 中获取续期任务的实例
        ExpirationEntry task = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        // 如果获取不到,代表已经取消
        if (task == null) {
            return;
        }
        if (threadId != null) {
            // ExpirationEntry 的线程计数器中移除此线程
            task.removeThreadId(threadId);
        }
        // 如果线程 id = null 或者续期任务没有线程
        if (threadId == null || task.hasNoThreads()) {
            // 尝试取消 netty 定时任务
            Timeout timeout = task.getTimeout();
            if (timeout != null) {
                timeout.cancel();
            }
            EXPIRATION_RENEWAL_MAP.remove(getEntryName());
        }
    }

    protected final <T> RFuture<T> evalWriteAsync(String key, Codec codec, RedisCommand<T> evalCommandType, String script, List<Object> keys, Object... params) {
        return commandExecutor.syncedEval(key, codec, evalCommandType, script, keys, params);
    }

    protected void acquireFailed(long waitTime, TimeUnit unit, long threadId) {
        commandExecutor.get(acquireFailedAsync(waitTime, unit, threadId));
    }

    protected void trySuccessFalse(long currentThreadId, CompletableFuture<Boolean> result) {
        acquireFailedAsync(-1, null, currentThreadId).whenComplete((res, e) -> {
            if (e == null) {
                result.complete(false);
            } else {
                result.completeExceptionally(e);
            }
        });
    }

    protected CompletableFuture<Void> acquireFailedAsync(long waitTime, TimeUnit unit, long threadId) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Condition newCondition() {
        // TODO implement
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocked() {
        return isExists();
    }
    
    @Override
    public RFuture<Boolean> isLockedAsync() {
        return isExistsAsync();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return isHeldByThread(Thread.currentThread().getId());
    }

    @Override
    public boolean isHeldByThread(long threadId) {
        RFuture<Boolean> future = commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.HEXISTS, getRawName(), getLockName(threadId));
        return get(future);
    }

    private static final RedisCommand<Integer> HGET = new RedisCommand<Integer>("HGET", new MapValueDecoder(), new IntegerReplayConvertor(0));
    
    public RFuture<Integer> getHoldCountAsync() {
        return commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, HGET, getRawName(), getLockName(Thread.currentThread().getId()));
    }
    
    @Override
    public int getHoldCount() {
        return get(getHoldCountAsync());
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return forceUnlockAsync();
    }

    @Override
    public RFuture<Void> unlockAsync() {
        long threadId = Thread.currentThread().getId();
        return unlockAsync(threadId);
    }

    @Override
    public RFuture<Void> unlockAsync(long threadId) {
        return getServiceManager().execute(() -> unlockAsync0(threadId));
    }

    /**
     * 解锁异步
     * @param threadId 线程 id
     */
    private RFuture<Void> unlockAsync0(long threadId) {
        // 解锁
        CompletionStage<Boolean> future = unlockInnerAsync(threadId);
        // CompletionStage.handle 产生异常以后依然会执行
        CompletionStage<Void> f = future.handle((opStatus, e) -> {
            // 取消本机的分布式锁续期任务
            cancelExpirationRenewal(threadId);

            // e != null 代表出现了异常
            if (e != null) {
                if (e instanceof CompletionException) {
                    throw (CompletionException) e;
                }
                // 抛出异常
                throw new CompletionException(e);
            }
            // 如果返回值为空
            if (opStatus == null) {
                IllegalMonitorStateException cause = new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                        + id + " thread-id: " + threadId);
                throw new CompletionException(cause);
            }
            return null;
        });

        return new CompletableFutureWrapper<>(f);
    }

    @Override
    public void unlock() {
        try {
            get(unlockAsync(Thread.currentThread().getId()));
        } catch (RedisException e) {
            if (e.getCause() instanceof IllegalMonitorStateException) {
                throw (IllegalMonitorStateException) e.getCause();
            } else {
                throw e;
            }
        }

//        Future<Void> future = unlockAsync();
//        future.awaitUninterruptibly();
//        if (future.isSuccess()) {
//            return;
//        }
//        if (future.cause() instanceof IllegalMonitorStateException) {
//            throw (IllegalMonitorStateException)future.cause();
//        }
//        throw commandExecutor.convertException(future);
    }

    @Override
    public boolean forceUnlock() {
        return get(forceUnlockAsync());
    }

    String getUnlockLatchName(String requestId) {
        return prefixName("redisson_unlock_latch", getRawName()) + ":" + requestId;
    }

    protected abstract RFuture<Boolean> unlockInnerAsync(long threadId, String requestId, int timeout);

    /**
     * 解锁
     */
    protected final RFuture<Boolean> unlockInnerAsync(long threadId) {
        String id = getServiceManager().generateId();
        MasterSlaveServersConfig config = getServiceManager().getConfig();
        int timeout = (config.getTimeout() + config.getRetryInterval()) * config.getRetryAttempts();
        timeout = Math.max(timeout, 1);
        // 解锁的实现类
        /** {@link RedissonLock#unlockInnerAsync(long, String, int)} */
        RFuture<Boolean> r = unlockInnerAsync(threadId, id, timeout);
        CompletionStage<Boolean> ff = r.thenApply(v -> {
            CommandAsyncExecutor ce = commandExecutor;
            if (ce instanceof CommandBatchService) {
                ce = new CommandBatchService(commandExecutor);
            }
            // 删除锁
            ce.writeAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.DEL, getUnlockLatchName(id));
            if (ce instanceof CommandBatchService) {
                ((CommandBatchService) ce).executeAsync();
            }
            return v;
        });
        return new CompletableFutureWrapper<>(ff);
    }

    @Override
    public RFuture<Void> lockAsync() {
        return lockAsync(-1, null);
    }

    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return lockAsync(leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Void> lockAsync(long currentThreadId) {
        return lockAsync(-1, null, currentThreadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync() {
        // 获取当前的线程 id. 作为参数值
        long tid = Thread.currentThread().getId();

        /** {@link RedissonLock#tryLockAsync(long)}*/
        return tryLockAsync(tid);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        return tryLockAsync(waitTime, -1, unit);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return tryLockAsync(waitTime, leaseTime, unit, currentThreadId);
    }

    protected final <T> CompletionStage<T> handleNoSync(long threadId, CompletionStage<T> ttlRemainingFuture) {
        // ttl 剩余
        /** {@link CommandAsyncService#handleNoSync(CompletionStage, Supplier)}*/
        return commandExecutor.handleNoSync(ttlRemainingFuture, () -> unlockInnerAsync(threadId));
    }

}
