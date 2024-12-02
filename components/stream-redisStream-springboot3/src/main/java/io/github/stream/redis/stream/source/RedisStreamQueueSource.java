package io.github.stream.redis.stream.source;

import io.github.stream.core.AbstractAutoRunnable;
import io.github.stream.core.Message;
import io.github.stream.core.configuration.ConfigContext;
import io.github.stream.core.message.MessageBuilder;
import io.github.stream.core.properties.BaseProperties;
import io.github.stream.core.source.AbstractSource;
import io.github.stream.redis.stream.Constants;
import io.github.stream.redis.stream.RedissonStateConfigure;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamReadGroupArgs;

import java.util.List;
import java.util.Map;

/**
 * redis stream 队列消费
 *
 * @author taowenwu
 * @date 2023-10-17 16:13:51
 * @since 1.0.0
 */
@Slf4j
public class RedisStreamQueueSource extends AbstractSource {

    private RedissonStateConfigure stateConfigure;

    private String[] topics;

    private List<RStream<Object, Object>> rTopics;
    /**
     * 监听器ID
     */
    private Map<String, Integer> topicListenerMap;
    /**
     *
     */
    private BaseProperties sourceConfig;
    /**
     * 拉取间隔
     */
    private Integer interval;
    /**
     * 执行线程
     */
    private Thread runnerThread;

    private RedisPollingRunner runner;

    @Override
    public void configure(ConfigContext context) {
        this.stateConfigure = RedissonStateConfigure.getInstance(context.getInstanceName());
        this.stateConfigure.configure(context);
        this.topics = stateConfigure.resolveTopic(context.getConfig());
        this.sourceConfig = context.getConfig();
        this.interval = context.getConfig().getInt("pollInterval", 50);
    }

    @Override
    public void start() {
        this.runner = new RedisPollingRunner();
        this.runnerThread = new Thread(runner, "kafka-source-runner");
        this.runner.startup();
        this.runnerThread.start();
        super.start();
    }

    @Override
    public void stop() {
        stateConfigure.getClient().shutdown();
        this.runner.shutdown();
    }


    private class RedisPollingRunner extends AbstractAutoRunnable {

        @Override
        public void runInternal() {

            while (isRunning()) {
                for (String topic : topics) {
                    RStream<Object, Object> stream = stateConfigure.getClient().getStream(topic);
                    rTopics.add(stream);
                    String groupName = sourceConfig.getString("groupName");
                    String consumeName = sourceConfig.getString("consumeName");

                    Map<StreamMessageId, Map<Object, Object>> streamMessageIdMapMap = stream.readGroup(groupName, consumeName
                            , StreamReadGroupArgs.neverDelivered());
                    for (Map.Entry<StreamMessageId, Map<Object, Object>> entry : streamMessageIdMapMap.entrySet()) {
                        StreamMessageId streamMessageId = entry.getKey();
                        Map<Object, Object> value = entry.getValue();
                        Message message = MessageBuilder.withPayload(value).setHeader(Constants.TOPIC_KEY, topic).build();
                        try {
                            getChannelProcessor().send(message);
                            stream.ackAsync(groupName, streamMessageId);
                        } catch (Exception e) {
                            log.error("redis stream ack error ,message:{}", message.getPayload(), e);
                        }
                    }
                }

                if (interval > 0) {
                    try {
                        Thread.sleep(interval);
                    } catch (InterruptedException ex) {
                        // 有可能调用interrupt会触发sleep interrupted异常
                        return;
                    }
                }
            }

        }
    }
}
