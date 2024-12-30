package io.github.stream.redis.stream.source;

import io.github.stream.core.AbstractAutoRunnable;
import io.github.stream.core.Message;
import io.github.stream.core.configuration.ConfigContext;
import io.github.stream.core.message.GenericMessage;
import io.github.stream.core.message.MessageBuilder;
import io.github.stream.core.message.MessageHeaders;
import io.github.stream.core.properties.BaseProperties;
import io.github.stream.core.source.AbstractSource;
import io.github.stream.redis.stream.RedisStreamConstants;
import io.github.stream.redis.stream.RedissonStateConfigure;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RStream;
import org.redisson.api.StreamGroup;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamReadGroupArgs;

import java.util.ArrayList;
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
    /**
     * redis stream 列表
     */
    private List<RStream<Object, Object>> streamList = new ArrayList<>();
    /**
     * source 配置
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
        for (String topic : topics) {
            RStream<Object, Object> stream = stateConfigure.getClient().getStream(topic);
            streamList.add(stream);
            stream.add(StreamAddArgs.entries("key1", "value", "key", "value"));
            String groupName = sourceConfig.getString("groupName");
            String consumeName = sourceConfig.getString("consumeName");
            List<StreamGroup> streamGroups = stream.listGroups();
            Boolean hasGroup = false;
            if (streamGroups.size() > 0) {
                for (StreamGroup streamGroup : streamGroups){
                    String name = streamGroup.getName();
                    if (name.equals(groupName)){
                        hasGroup = true;
                    }
                }
            }
            //  group 不存在就创建
            if (!hasGroup){
                stream.createGroup(StreamCreateGroupArgs.name(groupName).makeStream().entriesRead(sourceConfig.getInt("consumeBatch", 100)));
            }
            stream.createConsumer(groupName, consumeName);
        }
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
                    String groupName = sourceConfig.getString("groupName");
                    String consumeName = sourceConfig.getString("consumeName");
                    Map<StreamMessageId, Map<Object, Object>> streamMessageIdMapMap = stream.readGroup(groupName, consumeName
                            , StreamReadGroupArgs.neverDelivered());
                    for (Map.Entry<StreamMessageId, Map<Object, Object>> entry : streamMessageIdMapMap.entrySet()) {
                        StreamMessageId streamMessageId = entry.getKey();
                        Map<Object, Object> value = entry.getValue();
                        MessageHeaders header = (MessageHeaders) value.get("header");
                        Object body = value.get("body");
                        Message message = new GenericMessage(body, header);
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
