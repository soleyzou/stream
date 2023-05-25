/**
 * Copyright wendy512@yeah.net
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.github.stream.core.sink;

import java.util.List;

import io.github.stream.core.Sink;

/**
 * 默认实现
 * @author wendy512@yeah.net
 * @date 2023-05-19 10:21:42
 * @since 1.0.0
 */
public class DefaultSinkProcessor<T> extends AbstractSinkProcessor<T> {

    @Override
    public int process() {
        List<Sink<T>> sinks = getSinks();
        int processCount = 0;
        for (Sink<T> sink : sinks) {
            processCount += sink.process();
        }
        return processCount;
    }
}
