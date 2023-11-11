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
package org.redisson.client.protocol;

import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.decoder.MultiDecoder;
import org.redisson.misc.LogHelper;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * @param <T> input type
 * @param <R> output type
 */
public class CommandData<T, R> implements QueueCommand {
    /**
     * 封装 Redis 响应的结果
     */
    final CompletableFuture<R> promise;
    /**
     * Redis 命令
     */
    RedisCommand<T> command;
    /**
     * 命令的参数
     */
    final Object[] params;
    /**
     * 编解码器
     */
    final Codec codec;
    /**
     * 消息解码器
     */
    final MultiDecoder<Object> messageDecoder;

    public CommandData(CompletableFuture<R> promise/*返回值包装器*/,
                       Codec codec /*编解码器*/,
                       RedisCommand<T> command/*请求命令*/,
                       Object[] params /*参数*/) {
        this(promise, null, codec, command, params);
    }

    public CommandData(CompletableFuture<R> promise, MultiDecoder<Object> messageDecoder, Codec codec, RedisCommand<T> command, Object[] params) {
        this.promise = promise;
        this.command = command;
        this.params = params;
        this.codec = codec;
        this.messageDecoder = messageDecoder;
    }

    public RedisCommand<T> getCommand() {
        return command;
    }

    public Object[] getParams() {
        return params;
    }

    public MultiDecoder<Object> getMessageDecoder() {
        return messageDecoder;
    }

    public CompletableFuture<R> getPromise() {
        return promise;
    }

    /**
     * 抛出异常
     */
    public Throwable cause() {
        try {
            // 立即获取请求结果. 参数valueIfAbsent: 如果获取不到就返回 null
            promise.getNow(null);
            return null;
        } catch (CompletionException e) {
            return e.getCause();
        } catch (CancellationException e) {
            return e;
        }
    }

    /**
     * 判断任务有没有执行成功
     */
    public boolean isSuccess() {
        // 任务已完成 & 没有以异常的方式完成
        return promise.isDone() && !promise.isCompletedExceptionally();
    }

    /**
     * 尝试失败
     */
    public boolean tryFailure(Throwable cause) {
        return promise.completeExceptionally(cause);
    }

    public Codec getCodec() {
        return codec;
    }

    @Override
    public String toString() {
        return "CommandData [promise=" + promise + ", command=" + command + ", params="
                + LogHelper.toString(params) + ", codec=" + codec + "]";
    }

    /**
     * 获取发布订阅的操作
     */
    @Override
    public List<CommandData<Object, Object>> getPubSubOperations() {
        if (RedisCommands.PUBSUB_COMMANDS.contains(getCommand().getName())) {
            return Collections.singletonList((CommandData<Object, Object>) this);
        }
        return Collections.emptyList();
    }
    
    public boolean isBlockingCommand() {
        return command.isBlockingCommand();
    }

    @Override
    public boolean isExecuted() {
        return promise.isDone();
    }

}
