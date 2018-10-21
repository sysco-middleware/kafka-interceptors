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
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import  no.sysco.middleware.kafka.interceptor.config.utils.HarvesterInterceptorUtils;
import static no.sysco.middleware.kafka.interceptor.config.ConfigHarvesterInterceptorConfig.FILTERED_CONFIG_VALUES;



/**
 * Kafka interceptor for harvesting and storing Consumer user provided configs
 *
 * @author SYSCO Middleware
 * @param <K>
 * @param <V>
 */
public class ConsumerConfigHarvesterInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    private final Logger log;


    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String topicName = ConfigHarvesterInterceptorConfig.DEFAULT_TOPIC_NAME;

    //we need to remove interceptors or we are going into endless interception loop
    private static final String[] IGNORED_PROPERTIES = {
        ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
        StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG};


    public ConsumerConfigHarvesterInterceptor() {
        this.log = LoggerFactory.getLogger(ProducerConfigHarvesterInterceptor.class);
        MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    /**
     * Callback interceptor function
     *
     * @param configs user provided congiguration properties
     */
    @Override
    public void configure(final Map<String, ?> configs) {

        this.checkAndAssignTopicName(configs);

        try {
            final Properties props = new Properties();
            configs.forEach((k, v) -> {
                if (null != v && !Arrays.asList(IGNORED_PROPERTIES).contains(k)) {
                    props.put(k, v);
                }
            });

            configs.forEach((k, v) -> {
                if (Arrays.asList(FILTERED_CONFIG_VALUES).contains(k)) {
                    HarvesterInterceptorUtils.filterField(configs, k);
                }
            });

            //we need to add own serializers which are not presented when we we intercept consumer 
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            HarvesterInterceptorUtils.createTopic(this.topicName, 1, (short) 1, props);
            Producer<String, Object> producer = HarvesterInterceptorUtils.getProducer(props);
            //we send config not props so we have all original values
            String jsonizedConf = MAPPER.writeValueAsString(configs);

            producer.send(new ProducerRecord<>(this.topicName, props.getProperty("client.id"), jsonizedConf),
                    (metadata, exception) -> {
                        if (null != exception) {
                            this.log.error("Config send request failed", exception);
                        }
                        producer.close(100, TimeUnit.MILLISECONDS);
                    });
        } catch (JsonProcessingException ex) {
            this.log.error("Couldn't seriliaze config data to JSON", ex);
        }

    }

    /**
     * check if config contains {@link ProducerConfigHarvesterInterceptor#TOPIC_NAME} attribute, assign it as target topic and
     * removes from configs
     *
     * @param configs
     */
    private void checkAndAssignTopicName(final Map<String, ?> configs) {
        if (configs.containsKey(ConfigHarvesterInterceptorConfig.TOPIC_NAME)) {
            this.topicName = (String) configs.get(ConfigHarvesterInterceptorConfig.TOPIC_NAME);
        }
    }

    /**
     * onConsume callback
     *
     * @param records
     * @return
     */
    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        return records;
    }

    /**
     * Not used as we need only catch configuration properties during app initialzation
     *
     * @param offsets
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        //noop
    }

    /**
     * onClose callback
     */
    @Override
    public void close() {
        //noop
    }

}
