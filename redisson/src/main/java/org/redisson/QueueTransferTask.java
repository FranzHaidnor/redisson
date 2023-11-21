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
import org.redisson.api.RTopic;
import org.redisson.api.listener.BaseStatusListener;
import org.redisson.api.listener.MessageListener;
import org.redisson.client.codec.Codec;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.connection.ServiceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public abstract class QueueTransferTask {
    
    private static final Logger log = LoggerFactory.getLogger(QueueTransferTask.class);

    public static class TimeoutTask {
        
        private final long startTime;
        private final Timeout task;
        
        public TimeoutTask(long startTime, Timeout task) {
            super();
            this.startTime = startTime;
            this.task = task;
        }
        
        public long getStartTime() {
            return startTime;
        }
        
        public Timeout getTask() {
            return task;
        }
        
    }
    
    private int usage = 1;
    /**
     * 上一次执行的延迟任务
     */
    private final AtomicReference<TimeoutTask> lastTimeout = new AtomicReference<TimeoutTask>();
    private final ServiceManager serviceManager;
    
    public QueueTransferTask(ServiceManager serviceManager) {
        super();
        this.serviceManager = serviceManager;
    }

    public void incUsage() {
        usage++;
    }
    
    public int decUsage() {
        usage--;
        return usage;
    }
    
    private int messageListenerId;
    private int statusListenerId;
    
    public void start() {
        // 获取延迟任务的发布订阅主题
        RTopic schedulerTopic = getTopic();

        // 给订阅的通道添加监听器
        statusListenerId = schedulerTopic.addListener(new BaseStatusListener() {
            // 订阅成功以后执行
            @Override
            public void onSubscribe(String channel) {
                pushTask();
            }
        });

        messageListenerId = schedulerTopic.addListener(Long.class, new MessageListener<Long>() {
            // 如果接收到了消息
            @Override
            public void onMessage(CharSequence channel, Long startTime) {
                // 创建一个执行 Lua 脚本的定时任务
                scheduleTask(startTime);
            }
        });
    }
    
    public void stop() {
        RTopic schedulerTopic = getTopic();
        // 移除消息监听器
        schedulerTopic.removeListener(messageListenerId);
        // 移除状态监听器
        schedulerTopic.removeListener(statusListenerId);
    }

    /**
     * @param startTime ZSET 集合中最早的一个延迟任务的时间
     */
    private void scheduleTask(final Long startTime) {
        // 上一次执行的任务
        TimeoutTask oldTimeout = lastTimeout.get();
        if (startTime == null) {
            return;
        }
        
        if (oldTimeout != null) {
            oldTimeout.getTask().cancel();
        }

        // 剩余的延迟时间 = 预计执行时间 - 当前时间
        long delay = startTime - System.currentTimeMillis();
        // 如果延迟时间 > 10 毫秒就延迟执行, 否则立刻执行 Redis Lua 脚本
        if (delay > 10) {
            // 创建本地 Netty 定时任务
            Timeout timeout = serviceManager.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    // 提交任务
                    pushTask();

                    // 上一次执行任务的时间
                    TimeoutTask currentTimeout = lastTimeout.get();
                    // 如果为 null
                    if (currentTimeout.getTask() == timeout) {
                        // 设置为当前时间
                        lastTimeout.compareAndSet(currentTimeout, null);
                    }
                }
            }, delay, TimeUnit.MILLISECONDS);
            // 上一次的执行任务重新赋值
            if (!lastTimeout.compareAndSet(oldTimeout, new TimeoutTask(startTime, timeout))) {
                // 如果失败则取消.. 这里要是失败了,就不会一直循环执行了.  所以什么时候会失败?
                timeout.cancel();
            }
        } else {
            // 立刻执行
            pushTask();
        }
    }
    
    protected abstract RTopic getTopic();
    
    protected abstract RFuture<Long> pushTaskAsync();
    
    private void pushTask() {
        // 执行 Lua 脚本
        /** {@link RedissonDelayedQueue#RedissonDelayedQueue(QueueTransferService, Codec, CommandAsyncExecutor, String)}*/
        RFuture<Long> startTimeFuture = pushTaskAsync();
        // 无论成功还是失败,都会执行
        // res 是 startTime
        startTimeFuture.whenComplete((res, e) -> {
            // 如果出现异常
            if (e != null) {
                if (e instanceof RedissonShutdownException) {
                    return;
                }
                log.error(e.getMessage(), e);
                scheduleTask(System.currentTimeMillis() + 5 * 1000L);
                return;
            }
            // 如果成功,有返回值
            if (res != null) {
                scheduleTask(res);
            }
        });
    }

}
