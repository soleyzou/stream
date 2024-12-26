package io.github.stream.redis.stream.sink;

import io.github.stream.core.Message;
import io.github.stream.core.configuration.ConfigContext;
import io.github.stream.core.properties.BaseProperties;
import io.github.stream.core.sink.AbstractSink;
import io.github.stream.redis.stream.Constants;
import io.github.stream.redis.stream.RedissonStateConfigure;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamTrimArgs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * redis stream 队列推送
 * @author taowenwu
 * @date 2023-10-17 16:12:33
 * @since 1.0.0
 */
@Slf4j
public class RedisStreamQueueSink extends AbstractSink<Object> {

    private RedissonStateConfigure stateConfigure;

    private Map<String, RStream<String, Object>> streamTopics = new ConcurrentHashMap<>();

    private BaseProperties sinkProperties;

    @Override
    public void configure(ConfigContext context) {
        this.stateConfigure = RedissonStateConfigure.getInstance(context.getInstanceName());
        this.stateConfigure.configure(context);
        this.sinkProperties = context.getConfig();
    }

    @Override
    public void process(List<Message<Object>> messages) {
        for (Message message : messages) {
            String topic = message.getHeaders().getString(Constants.TOPIC_KEY);
            Object payload = message.getPayload();
            if (StringUtils.isBlank(topic)) {
                continue;
            }
            RStream<String, Object> stream;
            if (!streamTopics.containsKey(topic)) {
                stream = stateConfigure.getClient().getStream(topic);
                streamTopics.put(topic, stream);
            } else {
                stream = streamTopics.get(topic);
            }
            // 队列大小
            int maxSizeThreshold = sinkProperties.getInt("maxSizeThreshold", 100000);

            stream.add(StreamMessageId.AUTO_GENERATED, StreamAddArgs.entries("header",message.getHeaders(),"body",payload));
            stream.trimNonStrict(StreamTrimArgs.maxLen(maxSizeThreshold).limit(500000));
        }
    }

    @Override
    public void stop() {
        stateConfigure.getClient().shutdown();
        super.stop();
    }
}
