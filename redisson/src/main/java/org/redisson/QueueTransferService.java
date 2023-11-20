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

    public static void main(String[] args) {
        ConcurrentMap<String, QueueTransferTask> tasks = new ConcurrentHashMap<>();

        QueueTransferTask oldTask = tasks.putIfAbsent("name", new QueueTransferTask(null) {
            @Override
            protected RTopic getTopic() {
                return null;
            }

            @Override
            protected RFuture<Long> pushTaskAsync() {
                return null;
            }
        });
        System.out.println(oldTask);
    }

    public synchronized void schedule(String name, QueueTransferTask task) {
        // putIfAbsent 相当于.
        // if (!map.containsKey(key))
        //     return map.put(key, value);
        // else
        //     return map.get(key);

        QueueTransferTask oldTask = tasks.putIfAbsent(name, task);
        if (oldTask == null) {
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
                task.stop();
            }
        }
    }
    
    

}
