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
package org.redisson.client.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;
import org.redisson.client.WriteRedisConnectionException;
import org.redisson.client.protocol.QueueCommand;
import org.redisson.client.protocol.QueueCommandHolder;
import org.redisson.misc.LogHelper;

import java.util.Deque;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 *
 * @author Nikita Koksharov
 *
 */
public class CommandsQueue extends ChannelDuplexHandler {

    public static final AttributeKey<Deque<QueueCommandHolder>> COMMANDS_QUEUE = AttributeKey.valueOf("COMMANDS_QUEUE");

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        ctx.channel().attr(COMMANDS_QUEUE).set(new ConcurrentLinkedDeque<>());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Queue<QueueCommandHolder> queue = ctx.channel().attr(COMMANDS_QUEUE).get();
        Iterator<QueueCommandHolder> iterator = queue.iterator();
        while (iterator.hasNext()) {
            QueueCommandHolder command = iterator.next();
            if (command.getCommand().isBlockingCommand()) {
                continue;
            }

            iterator.remove();
            command.getChannelPromise().tryFailure(
                    new WriteRedisConnectionException("Channel has been closed! Can't write command: "
                                + LogHelper.toString(command.getCommand()) + " to channel: " + ctx.channel()));
        }

        super.channelInactive(ctx);
    }

    private final AtomicBoolean lock = new AtomicBoolean();

    /**
     * 往外面写
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof QueueCommand) {
            QueueCommand data = (QueueCommand) msg;
            // 创建一个命令持有者 QueueCommandHolder
            QueueCommandHolder holder = new QueueCommandHolder(data, promise);

            // 从 netty channel 中获取命令持有者队列
            Queue<QueueCommandHolder> queue = ctx.channel().attr(COMMANDS_QUEUE).get();

            while (true) {
                // 上锁. 保证请求顺序的不会错乱
                // 使用了原子类,做比较并交换. 预期值为 false, 上锁后新值为 true
                if (lock.compareAndSet(false, true)) {
                    try {
                        // 把命令持有者放进队列
                        queue.add(holder);
                        try {
                            // 给 ChannelPromise 添加监听器
                            holder.getChannelPromise().addListener(future -> {
                                // 如果没有执行成功, 就移除此次请求的 QueueCommandHolder
                                if (!future.isSuccess()) {
                                    queue.remove(holder);
                                }
                            });
                            // 向外写数据
                            ctx.writeAndFlush(data, holder.getChannelPromise());
                        } catch (Exception e) {
                            // 出现异常,移除此次请求的 QueueCommandHolder
                            queue.remove(holder);
                            throw e;
                        }
                    } finally {
                        // 解锁
                        lock.set(false);
                    }
                    break;
                }
            }
        } else {
            super.write(ctx, msg, promise);
        }
    }

}
