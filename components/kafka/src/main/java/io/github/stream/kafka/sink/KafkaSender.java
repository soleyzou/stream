package io.github.stream.kafka.sink;

import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import io.github.stream.core.lifecycle.AbstractLifecycleAware;
import io.github.stream.core.properties.AbstractProperties;

/**
 * kafka 消息发送
 * @author wendy512@yeah.net
 * @date 2023-05-24 11:20:47
 * @since 1.0.0
 */
public class KafkaSender extends AbstractLifecycleAware {

    private final KafkaProducer kafkaProducer;

    private static volatile KafkaSender instance;

    private KafkaSender(AbstractProperties properties) {
        Map config = properties.getConfig();
        if (null == config) {
            throw new IllegalArgumentException("Kafka sink config cannot empty");
        }
        config.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.kafkaProducer = new KafkaProducer(config);
    }

    public static KafkaSender getInstance(AbstractProperties properties) {
        if (null == instance) {
            synchronized (KafkaSender.class) {
                if (null == instance) {
                    instance = new KafkaSender(properties);
                    instance.start();
                }
            }
        }
        return instance;
    }

    public void send(String topic, Object payload) {
        ProducerRecord record = new ProducerRecord(topic, payload);
        kafkaProducer.send(record);
    }

    @Override
    public void stop() {
        kafkaProducer.close();
        super.stop();
    }
}
