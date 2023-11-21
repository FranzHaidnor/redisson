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
package org.redisson.connection;

import io.netty.buffer.ByteBuf;
import org.redisson.api.NodeType;
import org.redisson.client.RedisClient;
import org.redisson.misc.RedisURI;
import org.redisson.pubsub.PublishSubscribeService;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * ConnectionManager 是管理 Redis 连接的顶级接口
 *
 * @author Nikita Koksharov
 */
public interface ConnectionManager { // 连接管理器

    /**
     * 创建连接
     */
    void connect() throws InterruptedException;

    /**
     * 获取订阅服务
     */
    PublishSubscribeService getSubscribeService();

    /**
     * 获取最后一个集群节点
     */
    RedisURI getLastClusterNode();

    /**
     * 是否为集群模式
     */
    boolean isClusterMode();

    /**
     * 计算槽
     */
    int calcSlot(String key);

    int calcSlot(ByteBuf key);

    int calcSlot(byte[] key);

    /**
     * 获取连接实例集合
     */
    Collection<MasterSlaveEntry> getEntrySet();

    /**
     * 获取连接实例
     */
    MasterSlaveEntry getEntry(String name);

    MasterSlaveEntry getWriteEntry(int slot);

    MasterSlaveEntry getReadEntry(int slot);

    MasterSlaveEntry getEntry(InetSocketAddress address);

    MasterSlaveEntry getEntry(RedisURI addr);

    /**
     * 创建Redis 客户端
     */
    RedisClient createClient(NodeType type, InetSocketAddress address, RedisURI uri, String sslHostname);

    /**
     * 创建Redis 客户端
     */
    RedisClient createClient(NodeType type, RedisURI address, String sslHostname);

    /**
     * 通过 RedisClient 获取 MasterSlaveEntry
     */
    MasterSlaveEntry getEntry(RedisClient redisClient);

    /**
     * 关闭连接
     */
    void shutdown();

    /**
     * 关闭连接
     */
    void shutdown(long quietPeriod, long timeout, TimeUnit unit);

    /**
     * 获取连接管理器
     */
    ServiceManager getServiceManager();

}
