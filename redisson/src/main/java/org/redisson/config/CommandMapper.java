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
package org.redisson.config;

/**
 * 指令映射器
 * Maps Redis command names.
 *
 * @author Nikita Koksharov
 *
 */
public interface CommandMapper {

    /**
     * 将映射函数应用于输入 Redis 命令 <code>name<code>
     *
     * Applies map function to the input Redis command <code>name</code>
     *
     * @param name - original command name
     * @return mapped command name
     */
    String map(String name);

    /**
     * 返回输入的 Redis 命令名称。默认使用
     * Returns input Redis command name. Used by default
     *
     * @return NameMapper instance
     */
    static CommandMapper direct() {
        // 使用 CommandMapper 接口的静态方法创建 CommandMapper
        return new DefaultCommandMapper();
    }

}
