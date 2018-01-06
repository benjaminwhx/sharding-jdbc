/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingjdbc.core.keygen;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * 生成key的工厂类
 * 
 * @author zhangliang
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class KeyGeneratorFactory {
    
    /**
     * 创建key生成器
     * 
     * @param keyGeneratorClassName key生成器类名
     * @return key生成器实例
     */
    public static KeyGenerator newInstance(final String keyGeneratorClassName) {
        try {
            return (KeyGenerator) Class.forName(keyGeneratorClassName).newInstance();
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalArgumentException(String.format("Class %s should have public privilege and no argument constructor", keyGeneratorClassName));
        }
    }
}
