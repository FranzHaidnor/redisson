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
package org.redisson.command;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.FutureListener;
import org.redisson.RedissonShutdownException;
import org.redisson.ScanResult;
import org.redisson.api.NodeType;
import org.redisson.cache.LRUCacheMap;
import org.redisson.client.*;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.CommandData;
import org.redisson.client.protocol.CommandsData;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.connection.ClientConnectionsEntry;
import org.redisson.connection.ConnectionManager;
import org.redisson.connection.MasterSlaveEntry;
import org.redisson.connection.NodeSource;
import org.redisson.connection.NodeSource.Redirect;
import org.redisson.liveobject.core.RedissonObjectBuilder;
import org.redisson.misc.LogHelper;
import org.redisson.misc.RedisURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 *
 * @author Nikita Koksharov
 *
 * @param <V> type of value
 * @param <R> type of returned value
 */
@SuppressWarnings({"NestedIfDepth", "ParameterNumber"})
public class RedisExecutor<V, R> {

    static final Logger log = LoggerFactory.getLogger(RedisExecutor.class);

    // 是否为只读模式
    final boolean readOnlyMode;
    // redis 命令
    final RedisCommand<V> command;
    // 指令参数
    final Object[] params;
    // 主任务
    final CompletableFuture<R> mainPromise;
    // 是否忽略重定向
    final boolean ignoreRedirect;
    // RedissonObject 建造者
    final RedissonObjectBuilder objectBuilder;
    // redis 连接管理器
    final ConnectionManager connectionManager;
    // RedissonObjectBuilder 的参考类型
    final RedissonObjectBuilder.ReferenceType referenceType;
    // 是否重试
    final boolean noRetry;
    // redis 连接
    CompletableFuture<RedisConnection> connectionFuture;
    // redis 连接节点源
    NodeSource source;
    // redis 主从实例
    MasterSlaveEntry entry;
    // netty 编解码器
    Codec codec;
    // 尝试. 默认值等于 0
    volatile int attempt;
    // 超时
    volatile Optional<Timeout> timeout = Optional.empty();
    // 主任务监听器
    volatile BiConsumer<R, Throwable> mainPromiseListener;
    // 写任务
    volatile ChannelFuture writeFuture;
    // 请求异常
    volatile RedisException exception;

    int attempts;
    long retryInterval;
    long responseTimeout;

    public RedisExecutor(boolean readOnlyMode, NodeSource source, Codec codec, RedisCommand<V> command,
                         Object[] params, CompletableFuture<R> mainPromise, boolean ignoreRedirect,
                         ConnectionManager connectionManager, RedissonObjectBuilder objectBuilder,
                         RedissonObjectBuilder.ReferenceType referenceType, boolean noRetry) {
        super();
        this.readOnlyMode = readOnlyMode;
        this.source = source;
        this.codec = codec;
        this.command = command;
        this.params = params;
        this.mainPromise = mainPromise;
        this.ignoreRedirect = ignoreRedirect;
        this.connectionManager = connectionManager;
        this.objectBuilder = objectBuilder;
        this.noRetry = noRetry;

        this.attempts = connectionManager.getServiceManager().getConfig().getRetryAttempts();
        this.retryInterval = connectionManager.getServiceManager().getConfig().getRetryInterval();
        this.responseTimeout = connectionManager.getServiceManager().getConfig().getTimeout();
        this.referenceType = referenceType;
    }

    /**
     * K1 执行 Redis 命令
     */
    public void execute() {
        // 判断主任务是否取消
        if (mainPromise.isCancelled()) {
            free();  // 释放资源
            return;
        }

        // 如果连接关闭
        if (!connectionManager.getServiceManager().getShutdownLatch().acquire()) {
            // 释放资源
            free();
            // 抛出异常
            mainPromise.completeExceptionally(new RedissonShutdownException("Redisson is shutdown"));
            return;
        }

        try {
            // 获取编码器
            this.codec = getCodec(codec);

            // 获取连接
            CompletableFuture<RedisConnection> connectionFuture = getConnection();

            // 尝试异步
            CompletableFuture<R> attemptPromise = new CompletableFuture<>();

            // 主任务监听器
            mainPromiseListener = new BiConsumer<R, Throwable>() {
                @Override
                public void accept(R r, Throwable throwable) {
                    // 检查主任务是否已被取消
                    if (!mainPromise.isCancelled()) {
                        return;  // 如果任务没有取消就返回
                    }
                    // 取消执行连接任务成功
                    if (connectionFuture.cancel(false)) {
                        log.debug("Connection obtaining canceled for {}", command);
                        timeout.ifPresent(Timeout::cancel);
                        if (attemptPromise.cancel(false)) {
                            free();
                        }
                    }
                    // 如果取消执行连接任务失败
                    else {
                        if (command.isBlockingCommand()) {
                            RedisConnection c = connectionFuture.getNow(null);
                            if (writeFuture.cancel(false)) {
                                attemptPromise.cancel(false);
                            } else {
                                c.forceFastReconnectAsync().whenComplete((res, ex) -> {
                                    attemptPromise.cancel(true);
                                });
                            }
                        }
                    }
                }
            };

            if (attempt == 0) {
                // 当主任务结束后执行
                mainPromise.whenComplete((r, throwable) -> {
                    // 如果主任务监听器 != null
                    if (this.mainPromiseListener != null) {
                        this.mainPromiseListener.accept(r, throwable);
                    }
                });
            }

            // 定时重试超时
            scheduleRetryTimeout(connectionFuture, attemptPromise);
            // 定时连接超时
            scheduleConnectionTimeout(attemptPromise, connectionFuture);

            // 连接完成后回调
            connectionFuture.whenComplete((connection, e) -> {
                if (connectionFuture.isCancelled()) {
                    connectionManager.getServiceManager().getShutdownLatch().release();
                    return;
                }

                if (connectionFuture.isDone() && connectionFuture.isCompletedExceptionally()) {
                    connectionManager.getServiceManager().getShutdownLatch().release();
                    exception = convertException(connectionFuture);
                    if (attempt == attempts) {
                        attemptPromise.completeExceptionally(exception);
                    }
                    return;
                }

                try {
                    // 使用 netty 发送请求指令
                    sendCommand(attemptPromise, connection);
                } catch (Exception ex) {
                    free();
                    handleError(connectionFuture, e);
                    return;
                }

                scheduleWriteTimeout(attemptPromise);

                // 请求发送后的监听器
                writeFuture.addListener((ChannelFutureListener) future -> {
                    // 检查写入
                    checkWriteFuture(writeFuture, attemptPromise, connection);
                });
            });

            attemptPromise.whenComplete((r, e) -> {
                // 释放连接
                releaseConnection(attemptPromise, connectionFuture);
                // 检查尝试承诺
                checkAttemptPromise(attemptPromise, connectionFuture);
            });
        } catch (Exception e) {
            free();
            handleError(connectionFuture, e);
            throw e;
        }
    }

    private void scheduleConnectionTimeout(CompletableFuture<R> attemptPromise, CompletableFuture<RedisConnection> connectionFuture) {
        if (retryInterval > 0 && attempts > 0) {
            return;
        }

        timeout.ifPresent(Timeout::cancel);

        TimerTask task = timeout -> {
            if (connectionFuture.cancel(false)) {
                exception = new RedisTimeoutException("Unable to acquire connection! " + this.connectionFuture +
                        "Increase connection pool size or timeout. "
                        + "Node source: " + source
                        + ", command: " + LogHelper.toString(command, params)
                        + " after " + attempt + " retry attempts");

                attemptPromise.completeExceptionally(exception);
            }
        };

        timeout = Optional.of(connectionManager.getServiceManager().newTimeout(task, responseTimeout, TimeUnit.MILLISECONDS));
    }

    private void scheduleWriteTimeout(CompletableFuture<R> attemptPromise) {
        if (retryInterval > 0 && attempts > 0) {
            return;
        }

        timeout.ifPresent(Timeout::cancel);

        TimerTask task = timeout -> {
            if (writeFuture.cancel(false)) {
                exception = new RedisTimeoutException("Command still hasn't been written into connection! " +
                        "Check CPU load on the application side. Check connection with Redis node: " + connectionFuture.join().getRedisClient().getAddr() +
                        " for TCP packet drops. Try to increase nettyThreads setting. "
                        + " Node source: " + source + ", connection: " + connectionFuture.join()
                        + ", command: " + LogHelper.toString(command, params)
                        + " after " + attempt + " retry attempts");
                attemptPromise.completeExceptionally(exception);
            }
        };

        timeout = Optional.of(connectionManager.getServiceManager().newTimeout(task, responseTimeout, TimeUnit.MILLISECONDS));
    }

    private void scheduleRetryTimeout(CompletableFuture<RedisConnection> connectionFuture, CompletableFuture<R> attemptPromise) {
        if (retryInterval == 0 || attempts == 0) {
            return;
        }

        TimerTask retryTimerTask = new TimerTask() {

            @Override
            public void run(Timeout t) throws Exception {
                if (attemptPromise.isDone()) {
                    return;
                }

                if (connectionFuture.cancel(false)) {
                    exception = new RedisTimeoutException("Unable to acquire connection! " + connectionFuture +
                            "Increase connection pool size. "
                            + "Node source: " + source
                            + ", command: " + LogHelper.toString(command, params)
                            + " after " + attempt + " retry attempts");
                } else {
                    if (connectionFuture.isDone() && !connectionFuture.isCompletedExceptionally()) {
                        if (writeFuture == null || !writeFuture.isDone()) {
                            if (attempt == attempts) {
                                if (writeFuture != null && writeFuture.cancel(false)) {
                                    if (exception == null) {
                                        exception = new RedisTimeoutException("Command still hasn't been written into connection! " +
                                                "Check CPU load on the application side. Check connection with Redis node: " + getNow(connectionFuture).getRedisClient().getAddr() +
                                                " for TCP packet drops. Try to increase nettyThreads setting. "
                                                + " Node source: " + source + ", connection: " + getNow(connectionFuture)
                                                + ", command: " + LogHelper.toString(command, params)
                                                + " after " + attempt + " retry attempts");
                                    }
                                    attemptPromise.completeExceptionally(exception);
                                }
                                return;
                            }
                            attempt++;

                            scheduleRetryTimeout(connectionFuture, attemptPromise);
                            return;
                        }

                        if (writeFuture.isSuccess()) {
                            return;
                        }
                    }
                }

                if (mainPromise.isCancelled()) {
                    if (attemptPromise.cancel(false)) {
                        free();
                    }
                    return;
                }

                if (attempt == attempts) {
                    // filled out in connectionFuture or writeFuture handler
                    if (exception != null) {
                        attemptPromise.completeExceptionally(exception);
                    }
                    return;
                }
                if (!attemptPromise.cancel(false)) {
                    return;
                }

                attempt++;
                if (log.isDebugEnabled()) {
                    log.debug("attempt {} for command {} and params {} to {}",
                            attempt, command, LogHelper.toString(params), source);
                }

                mainPromiseListener = null;

                execute();
            }

        };

        timeout = Optional.of(connectionManager.getServiceManager().newTimeout(retryTimerTask, retryInterval, TimeUnit.MILLISECONDS));
    }

    protected void free() {
        free(params);
    }

    protected void free(Object[] params) {
        for (Object obj : params) {
            ReferenceCountUtil.safeRelease(obj);
        }
    }

    private void checkWriteFuture(ChannelFuture future, CompletableFuture<R> attemptPromise, RedisConnection connection) {
        if (future.isCancelled() || attemptPromise.isDone()) {
            return;
        }

        if (!future.isSuccess()) {
            exception = new WriteRedisConnectionException(
                    "Unable to write command into connection! Increase nettyThreads setting. Node source: "
                            + source + ", connection: " + connection +
                            ", command: " + LogHelper.toString(command, params)
                            + " after " + attempt + " retry attempts", future.cause());
            if (attempt == attempts) {
                attemptPromise.completeExceptionally(exception);
            }
            return;
        }

        scheduleResponseTimeout(attemptPromise, connection);
    }

    private void scheduleResponseTimeout(CompletableFuture<R> attemptPromise, RedisConnection connection) {
        timeout.ifPresent(Timeout::cancel);

        long timeoutTime = responseTimeout;
        if (command != null && command.isBlockingCommand()) {
            long popTimeout = 0;
            if (RedisCommands.BLOCKING_COMMANDS.contains(command)) {
                for (int i = 0; i < params.length - 1; i++) {
                    if ("BLOCK".equals(params[i])) {
                        popTimeout = Long.valueOf(params[i + 1].toString());
                        break;
                    }
                }
            } else {
                popTimeout = Long.valueOf(params[params.length - 1].toString()) * 1000;
            }

            handleBlockingOperations(attemptPromise, connection, popTimeout);
            if (popTimeout == 0) {
                return;
            }
            timeoutTime += popTimeout;
            // add 1 second due to issue https://github.com/antirez/redis/issues/874
            timeoutTime += 1000;
        }

        long timeoutAmount = timeoutTime;
        TimerTask timeoutResponseTask = timeout -> {
            if (isResendAllowed(attempt, attempts)) {
                if (!attemptPromise.cancel(false)) {
                    return;
                }

                connectionManager.getServiceManager().newTimeout(t -> {
                    attempt++;
                    if (log.isDebugEnabled()) {
                        log.debug("response timeout. new attempt {} for command {} and params {} node {}",
                                attempt, command, LogHelper.toString(params), source);
                    }

                    mainPromiseListener = null;
                    execute();
                }, retryInterval, TimeUnit.MILLISECONDS);
                return;
            }

            attemptPromise.completeExceptionally(
                    new RedisResponseTimeoutException("Redis server response timeout (" + timeoutAmount + " ms) occured"
                            + " after " + attempt + " retry attempts,"
                            + " is non-idempotent command: " + (command != null && command.isNoRetry())
                            + " Check connection with Redis node: " + connection.getRedisClient().getAddr() + " for TCP packet drops or bandwidth limits. "
                            + " Try to increase nettyThreads and/or timeout settings. Command: "
                            + LogHelper.toString(command, params) + ", channel: " + connection.getChannel()));
        };

        timeout = Optional.of(connectionManager.getServiceManager().newTimeout(timeoutResponseTask, timeoutTime, TimeUnit.MILLISECONDS));
    }

    protected boolean isResendAllowed(int attempt, int attempts) {
        return attempt < attempts
                && !noRetry
                && (command == null || (!command.isBlockingCommand() && !command.isNoRetry()));
    }

    private void handleBlockingOperations(CompletableFuture<R> attemptPromise, RedisConnection connection, long popTimeout) {
        FutureListener<Void> listener = f -> {
            mainPromise.completeExceptionally(new RedissonShutdownException("Redisson is shutdown"));
        };

        Timeout scheduledFuture;
        if (popTimeout != 0) {
            // handling cases when connection has been lost
            scheduledFuture = connectionManager.getServiceManager().newTimeout(timeout -> {
                if (attemptPromise.complete(null)) {
                    connection.forceFastReconnectAsync();
                }
            }, popTimeout + 3000, TimeUnit.MILLISECONDS);
        } else {
            scheduledFuture = null;
        }

        mainPromise.whenComplete((res, e) -> {
            if (scheduledFuture != null) {
                scheduledFuture.cancel();
            }

            synchronized (listener) {
                connectionManager.getServiceManager().getShutdownPromise().removeListener(listener);
            }

            // handling cancel operation for blocking commands
            if ((mainPromise.isCancelled()
                    || e instanceof InterruptedException)
                    && !attemptPromise.isDone()) {
                log.debug("Canceled blocking operation {} used {}", command, connection);
                connection.forceFastReconnectAsync().whenComplete((r, ex) -> {
                    attemptPromise.cancel(true);
                });
                return;
            }

            if (e instanceof RedissonShutdownException) {
                attemptPromise.completeExceptionally(e);
            }
        });

        synchronized (listener) {
            if (!mainPromise.isDone()) {
                connectionManager.getServiceManager().getShutdownPromise().addListener(listener);
            }
        }
    }

    protected Throwable cause(CompletableFuture<?> future) {
        try {
            future.getNow(null);
            return null;
        } catch (CompletionException ex2) {
            return ex2.getCause();
        } catch (CancellationException ex1) {
            return ex1;
        }
    }

    protected void checkAttemptPromise(CompletableFuture<R> attemptFuture, CompletableFuture<RedisConnection> connectionFuture) {
        timeout.ifPresent(Timeout::cancel);

        if (attemptFuture.isCancelled()) {
            return;
        }

        try {
            mainPromiseListener = null;

            Throwable cause = cause(attemptFuture);
            if (cause instanceof RedisMovedException && !ignoreRedirect) {
                RedisMovedException ex = (RedisMovedException) cause;
                if (source.getRedirect() == Redirect.MOVED
                        && source.getAddr().equals(ex.getUrl())) {
                    mainPromise.completeExceptionally(new RedisException("MOVED redirection loop detected. Node " + source.getAddr() + " has further redirect to " + ex.getUrl()));
                    return;
                }

                onException();

                CompletableFuture<RedisURI> ipAddrFuture = connectionManager.getServiceManager().resolveIP(ex.getUrl());
                ipAddrFuture.whenComplete((ip, e) -> {
                    if (e != null) {
                        free();
                        handleError(connectionFuture, e);
                        return;
                    }
                    source = new NodeSource(ex.getSlot(), ip, Redirect.MOVED);
                    execute();
                });
                return;
            }

            if (cause instanceof RedisAskException && !ignoreRedirect) {
                RedisAskException ex = (RedisAskException) cause;

                onException();

                CompletableFuture<RedisURI> ipAddrFuture = connectionManager.getServiceManager().resolveIP(ex.getUrl());
                ipAddrFuture.whenComplete((ip, e) -> {
                    if (e != null) {
                        free();
                        handleError(connectionFuture, e);
                        return;
                    }
                    source = new NodeSource(ex.getSlot(), ip, Redirect.ASK);
                    execute();
                });
                return;
            }

            if (cause instanceof RedisLoadingException) {
                RedisConnection connection = connectionFuture.getNow(null);
                if (connection != null) {
                    ClientConnectionsEntry ce = entry.getEntry(connection.getRedisClient());
                    if (ce != null && ce.getNodeType() == NodeType.SLAVE) {
                        source = new NodeSource(entry.getClient());
                        execute();
                        return;
                    }
                }
            }

            if (cause instanceof RedisRetryException) {
                if (attempt < attempts) {
                    onException();
                    connectionManager.getServiceManager().newTimeout(timeout -> {
                        attempt++;
                        execute();
                    }, retryInterval, TimeUnit.MILLISECONDS);
                    return;
                }
            }

            free();

            handleResult(attemptFuture, connectionFuture);

        } catch (Exception e) {
            handleError(connectionFuture, e);
        }
    }

    protected void handleResult(CompletableFuture<R> attemptPromise, CompletableFuture<RedisConnection> connectionFuture) throws ReflectiveOperationException {
        R res;
        try {
            res = attemptPromise.getNow(null);
        } catch (CompletionException e) {
            handleError(connectionFuture, e.getCause());
            return;
        } catch (CancellationException e) {
            handleError(connectionFuture, e);
            return;
        }

        if (res instanceof ScanResult) {
            ((ScanResult) res).setRedisClient(getNow(connectionFuture).getRedisClient());
        }

        handleSuccess(mainPromise, connectionFuture, res);
    }

    protected void onException() {
    }

    protected void handleError(CompletableFuture<RedisConnection> connectionFuture, Throwable cause) {
        mainPromise.completeExceptionally(cause);
        RedisClient client = connectionFuture.join().getRedisClient();
        client.getConfig().getFailedNodeDetector().onCommandFailed(cause);
        if (client.getConfig().getFailedNodeDetector().isNodeFailed()) {
            entry.shutdownAndReconnectAsync(client, cause);
        }
    }

    protected void handleSuccess(CompletableFuture<R> promise, CompletableFuture<RedisConnection> connectionFuture, R res) throws ReflectiveOperationException {
        if (objectBuilder != null) {
            promise.complete((R) objectBuilder.tryHandleReference(res, referenceType));
        } else {
            promise.complete(res);
        }
        connectionFuture.join().getRedisClient().getConfig().getFailedNodeDetector().onCommandSuccessful();
    }

    protected void sendCommand(CompletableFuture<R> attemptPromise, RedisConnection connection) {
        if (source.getRedirect() == Redirect.ASK) {
            List<CommandData<?, ?>> list = new ArrayList<>(2);
            CompletableFuture<Void> promise = new CompletableFuture<>();
            list.add(new CommandData<>(promise, codec, RedisCommands.ASKING, new Object[]{}));
            list.add(new CommandData<>(attemptPromise, codec, command, params));
            CompletableFuture<Void> main = new CompletableFuture<>();
            //  使用 netty 发送请求 channel.writeAndFlush(data)
            writeFuture = connection.send(new CommandsData(main, list, false, false));
        } else {
            if (log.isDebugEnabled()) {
                String connectionType = " ";
                if (connection instanceof RedisPubSubConnection) {
                    connectionType = " pubsub ";
                }
                log.debug("acquired{}connection for command {} and params {} from slot {} using node {}... {}",
                        connectionType, command, LogHelper.toString(params), source, connection.getRedisClient().getAddr(), connection);
            }
            //  使用 netty 发送请求 channel.writeAndFlush(data)
            writeFuture = connection.send(new CommandData<>(attemptPromise, codec, command, params));

            if (connectionManager.getServiceManager().getConfig().getMasterConnectionPoolSize() < 10
                    && !command.isBlockingCommand()) {
                release(connection);
            }
        }
    }

    protected void releaseConnection(CompletableFuture<R> attemptPromise, CompletableFuture<RedisConnection> connectionFuture) {
        if (connectionFuture.isDone() && connectionFuture.isCompletedExceptionally()) {
            return;
        }

        RedisConnection connection = getNow(connectionFuture);
        connectionManager.getServiceManager().getShutdownLatch().release();
        if (connectionManager.getServiceManager().getConfig().getMasterConnectionPoolSize() < 10) {
            if (source.getRedirect() == Redirect.ASK
                    || getClass() != RedisExecutor.class
                    || (command != null && command.isBlockingCommand())) {
                release(connection);
            }
        } else {
            release(connection);
        }

        if (log.isDebugEnabled()) {
            String connectionType = " ";
            if (connection instanceof RedisPubSubConnection) {
                connectionType = " pubsub ";
            }

            log.debug("connection{}released for command {} and params {} from slot {} using connection {}",
                    connectionType, command, LogHelper.toString(params), source, connection);
        }
    }

    private void release(RedisConnection connection) {
        if (readOnlyMode) {
            entry.releaseRead(connection);
        } else {
            entry.releaseWrite(connection);
        }
    }

    public RedisClient getRedisClient() {
        return getNow(connectionFuture).getRedisClient();
    }

    protected CompletableFuture<RedisConnection> getConnection() {
        if (readOnlyMode) {
            connectionFuture = connectionReadOp(command);
        } else {
            connectionFuture = connectionWriteOp(command);
        }
        return connectionFuture;
    }

    private static final Map<ClassLoader, Map<Codec, Codec>> CODECS = new LRUCacheMap<>(25, 0, 0);

    protected Codec getCodec(Codec codec) {
        if (codec == null) {
            return null;
        }

        if (!connectionManager.getServiceManager().getCfg().isUseThreadClassLoader()) {
            return codec;
        }

        for (Class<?> clazz : BaseCodec.SKIPPED_CODECS) {
            if (clazz.isAssignableFrom(codec.getClass())) {
                return codec;
            }
        }

        Codec codecToUse = codec;
        ClassLoader threadClassLoader = Thread.currentThread().getContextClassLoader();
        if (threadClassLoader != null) {
            Map<Codec, Codec> map = CODECS.computeIfAbsent(threadClassLoader, k ->
                    new LRUCacheMap<>(200, 0, 0));
            codecToUse = map.get(codec);
            if (codecToUse == null) {
                try {
                    codecToUse = codec.getClass().getConstructor(ClassLoader.class, codec.getClass()).newInstance(threadClassLoader, codec);
                } catch (NoSuchMethodException e) {
                    codecToUse = codec;
                    // skip
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                map.put(codec, codecToUse);
            }
        }
        return codecToUse;
    }

    protected <T> T getNow(CompletableFuture<T> future) {
        try {
            return future.getNow(null);
        } catch (Exception e) {
            return null;
        }
    }

    protected <T> RedisException convertException(CompletableFuture<T> future) {
        Throwable cause = cause(future);
        if (cause instanceof RedisException) {
            return (RedisException) cause;
        }
        return new RedisException("Unexpected exception while processing command", cause);
    }

    final CompletableFuture<RedisConnection> connectionReadOp(RedisCommand<?> command) {
        entry = getEntry(true);
        if (entry == null) {
            CompletableFuture<RedisConnection> f = new CompletableFuture<>();
            f.completeExceptionally(connectionManager.getServiceManager().createNodeNotFoundException(source));
            return f;
        }

        if (source.getRedirect() != null) {
            return entry.connectionReadOp(command, source.getAddr());
        }
        if (source.getRedisClient() != null) {
            return entry.connectionReadOp(command, source.getRedisClient());
        }

        return entry.connectionReadOp(command);
    }

    final CompletableFuture<RedisConnection> connectionWriteOp(RedisCommand<?> command) {
        entry = getEntry(false);
        if (entry == null) {
            CompletableFuture<RedisConnection> f = new CompletableFuture<>();
            f.completeExceptionally(connectionManager.getServiceManager().createNodeNotFoundException(source));
            return f;
        }
        // fix for https://github.com/redisson/redisson/issues/1548
        if (source.getRedirect() != null
                && !source.getAddr().equals(entry.getClient().getAddr())
                && entry.hasSlave(source.getAddr())) {
            return entry.redirectedConnectionWriteOp(command, source.getAddr());
        }
        return entry.connectionWriteOp(command);
    }

    private MasterSlaveEntry getEntry(boolean read) {
        if (source.getRedirect() != null) {
            return connectionManager.getEntry(source.getAddr());
        }

        MasterSlaveEntry entry = source.getEntry();
        if (source.getRedisClient() != null) {
            entry = connectionManager.getEntry(source.getRedisClient());
        }
        if (entry == null && source.getSlot() != null) {
            if (read) {
                entry = connectionManager.getReadEntry(source.getSlot());
            } else {
                entry = connectionManager.getWriteEntry(source.getSlot());
            }
        }
        return entry;
    }

}
