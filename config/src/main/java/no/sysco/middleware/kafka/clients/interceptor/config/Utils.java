/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package no.sysco.middleware.kafka.clients.interceptor.config;

import java.util.Collections;
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
 * @author 100tsa
 */
public class Utils {

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
     *
     * @param props
     * @return
     */
    public static final Producer<String, Object> getProducer(final Properties props) {
        return new KafkaProducer<>(props);
    }
}
