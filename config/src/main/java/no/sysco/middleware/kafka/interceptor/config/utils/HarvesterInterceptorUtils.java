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
package no.sysco.middleware.kafka.interceptor.config.utils;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 *
 * @author Sysco Middleware AS
 */
public class HarvesterInterceptorUtils {

    /**
     * Create new topic if not exists
     *
     * @param name topic name
     * @param partitions
     * @param rf replication factor
     * @param prop properties
     */
    public static final void createTopic(final String name, final int partitions, final short rf, final Properties prop) {
        try (final AdminClient adminClient = KafkaAdminClient.create(prop)) {
            try {
                final NewTopic newTopic = new NewTopic(name, partitions, rf);
                final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
                createTopicsResult.values().get(name).get();
            } catch (InterruptedException | ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw new RuntimeException(e.getMessage(), e);
                }
                // TopicExistsException - Swallow this exception, just means the topic already exists.
            }
        }
    }

    /**
     * Create and return new KafkaProducer instance
     *
     * @param props Propertiese object with KafkaProducer configuration
     * @return new KafkaProducer instance
     */
    public static final Producer<String, Object> getProducer(final Properties props) {
        return new KafkaProducer<>(props);
    }

    /**
     * Replace value by given key k with masked string
     *
     * @param <T>
     * @param configs
     * @param k
     */
    public static <T> void filterField(Map<String, T> configs, String k) {
        configs.replace(k, (T) "XXXX-XXX-XXX-XXX");
    }
    
}
