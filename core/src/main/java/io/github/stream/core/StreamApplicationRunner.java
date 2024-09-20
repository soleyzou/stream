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

package io.github.stream.core;

import io.github.stream.core.configuration.MaterializedConfiguration;
import io.github.stream.core.lifecycle.AbstractLifecycleAware;
import lombok.extern.slf4j.Slf4j;


/**
 * 启动器
 * @author wendy512@yeah.net
 * @date 2023-05-22 10:32:24
 * @since 1.0.0
 */
@Slf4j
public class StreamApplicationRunner extends AbstractLifecycleAware implements ApplicationRunner {

    private MaterializedConfiguration configuration;

    @Override
    public void start() {
        this.configuration.getChannels().forEach((name, channels) -> channels.forEach(Channel::start));
        this.configuration.getSinkRunners().forEach((name, runners) -> runners.forEach(SinkRunner::start));
        this.configuration.getSources().values().forEach(Source::start);
        super.start();
    }

    @Override
    public void stop() {
        configuration.getSources().forEach((name, source) -> {
            source.stop();
            log.info("Source {} stopping", name);
        });

        configuration.getSinkRunners().forEach((name, runners) -> {
            log.info("Sink runner {} stopping", name);
            runners.forEach(SinkRunner::stop);
        });

        configuration.getChannels().forEach((name, channels) -> {
            log.info("Channel {} stopping", name);
            channels.forEach(Channel::stop);
        });
        super.stop();
    }

    @Override
    public void destroy() throws Exception {
        stop();
    }

    @Override
    public void setConfiguration(MaterializedConfiguration configuration) {
        this.configuration = configuration;
    }
}
