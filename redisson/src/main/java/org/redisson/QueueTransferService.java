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

import org.redisson.api.RFuture;
import org.redisson.api.RTopic;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class QueueTransferService {

    private final ConcurrentMap<String, QueueTransferTask> tasks = new ConcurrentHashMap<>();

    public synchronized void schedule(String name, QueueTransferTask task) {
        // 返回值 与指定键关联的前一个值；如果该键没有映射，则返回null 。 （如果实现支持 null 值，则null返回还可以指示映射先前将null与键关联。）
        QueueTransferTask oldTask = tasks.putIfAbsent(name, task);
        // 如果之前 map 中没有
        if (oldTask == null) {
            // 则把这次放进去的任务启动
            task.start();
        } else {
            // 使用计数器自增
            oldTask.incUsage();
        }
    }
    
    public synchronized void remove(String name) {
        QueueTransferTask task = tasks.get(name);
        if (task != null) {
            if (task.decUsage() == 0) {
                tasks.remove(name, task);
                // 停止任务
                task.stop();
            }
        }
    }
    
    

}
