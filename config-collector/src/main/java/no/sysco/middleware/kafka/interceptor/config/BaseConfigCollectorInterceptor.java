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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static no.sysco.middleware.kafka.interceptor.config.ConfigCollectorInterceptorConfig.CONFIG_COLLECTOR_BLACKLIST_DEFAULT;

/**
 * @author Sysco Middleware AS
 */
class BaseConfigCollectorInterceptor implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseConfigCollectorInterceptor.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    BaseConfigCollectorInterceptor() {
        MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    /**
     * Callback interceptor function
     *
     * @param configs user provided configuration properties
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        final String topicName;
        if (configs.containsKey(ConfigCollectorInterceptorConfig.CONFIG_COLLECTOR_TOPIC_CONFIG)) {
            topicName = (String) configs.get(ConfigCollectorInterceptorConfig.CONFIG_COLLECTOR_TOPIC_CONFIG);
        } else {
            topicName = ConfigCollectorInterceptorConfig.CONFIG_COLLECTOR_TOPIC_DEFAULT;
        }

        try {
            final HashMap<String, Object> props = new HashMap<>();
            configs.forEach((k, v) -> {
                if (!k.contains(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG)) {
                    props.remove(k);
                }
            });

            //we need to add own serializers to ensure we serialize string data
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            final Map<String, Object> filteredConfigs = new HashMap<>();
            configs.forEach((k, v) -> {
                if (CONFIG_COLLECTOR_BLACKLIST_DEFAULT.contains(k)) {
                    filteredConfigs.put(k, "XXXX-XXX-XXX-XXX");
                } else {
                    filteredConfigs.put(k, v);
                }
            });
            Producer<String, Object> producer = new KafkaProducer<>(props);
            //we send config not props so we have all original values
            String json = MAPPER.writeValueAsString(filteredConfigs);

            final ProducerRecord<String, Object> record =
                    new ProducerRecord<>(topicName, (String) props.get("client.id"), json);
            producer.send(
                    record,
                    (metadata, exception) -> {
                        if (Objects.nonNull(exception)) {
                            LOGGER.error("Config Collector Interceptor failed", exception);
                        }
                    }).get();
            producer.close();
        } catch (JsonProcessingException | InterruptedException | ExecutionException ex) {
            LOGGER.error("Could not serialize config data to JSON", ex);
        }

    }
}
