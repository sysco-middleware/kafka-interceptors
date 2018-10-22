/*
 * The MIT License
 *
 * Copyright 2018 Sysco Middleware AS.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package no.sysco.middleware.kafka.interceptor.config;

import no.sysco.middleware.kafka.interceptor.config.proto.ClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static no.sysco.middleware.kafka.interceptor.config.ConfigCollectorInterceptorConfig.CONFIG_COLLECTOR_BLACKLIST_DEFAULT;

/**
 * @author Sysco Middleware AS
 */
class BaseConfigCollectorInterceptor implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseConfigCollectorInterceptor.class.getName());

    private final ClientConfig.ClientType clientType;

    BaseConfigCollectorInterceptor(ClientConfig.ClientType clientType) {
        this.clientType = clientType;
    }

    /**
     * Callback interceptor function
     *
     * @param configs user provided configuration properties
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        try {
            final ProducerRecord<String, byte[]> record = buildConfigRecord(configs);
            final Producer<String, byte[]> producer = buildConfigProducer(configs);
            producer.send(record).get();
            producer.close();
        } catch (Exception ex) {
            LOGGER.error("Config Collector Interceptor failed", ex);
        }
    }

    private Producer<String, byte[]> buildConfigProducer(Map<String, ?> configs) {
        final HashMap<String, Object> configProducerConfigs = new HashMap<>();
        configs.forEach((k, v) -> {
            if (!k.contains(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG)) {
                configProducerConfigs.put(k, v);
            }
        });
        configProducerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProducerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return new KafkaProducer<>(configProducerConfigs);
    }

    private ProducerRecord<String, byte[]> buildConfigRecord(Map<String, ?> configs) {
        final String topicName = configTopic(configs);
        final String clientId = (String) configs.get("client.id");
        final ClientConfig.Builder builder = ClientConfig.newBuilder();
        builder.setType(clientType).setId(clientId);
        configs.forEach((k, v) -> {
            if (CONFIG_COLLECTOR_BLACKLIST_DEFAULT.contains(k)) {
                builder.putEntries(k, "<value>");
            } else {
                builder.putEntries(k, v.toString());
            }
        });
        final String key = String.format("%s-%s", clientType.name(), clientId);
        final byte[] value = builder.build().toByteArray();
        return new ProducerRecord<>(topicName, key, value);
    }

    private String configTopic(Map<String, ?> configs) {
        final String topicName;
        final Object maybeTopicName = configs.get(ConfigCollectorInterceptorConfig.CONFIG_COLLECTOR_TOPIC_CONFIG);
        if (Objects.nonNull(maybeTopicName) && maybeTopicName instanceof String) {
            topicName = (String) maybeTopicName;
        } else {
            topicName = ConfigCollectorInterceptorConfig.CONFIG_COLLECTOR_TOPIC_DEFAULT;
        }
        return topicName;
    }
}
