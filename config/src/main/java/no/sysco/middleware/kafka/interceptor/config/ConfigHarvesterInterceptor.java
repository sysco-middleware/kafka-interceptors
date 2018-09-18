package no.sysco.middleware.kafka.interceptor.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka interceptor for harvesting and storing Producer/Consumer/KafkaStreams user provided configs
 *
 * @author SYSCO Middleware
 * @param <K>
 * @param <V>
 */
public class ConfigHarvesterInterceptor<K, V> implements ProducerInterceptor<K, V>, ConsumerInterceptor<K, V> {

    private final Logger log;

    private static final String DEFAULT_TOPIC_NAME = "__client_configs";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String topicName = DEFAULT_TOPIC_NAME;

    //we need to remove interceptors or we are going into endless interception loop
    private static final String[] IGNORED_PROPERTIES = {
        ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
        ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
        StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
        StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG};

    private static final String[] FILTERED_CONFIG_VALUES = new String[]{
        "ssl.key.password", "ssl.keystore.password"
    };

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

    public ConfigHarvesterInterceptor() {
        this.log = LoggerFactory.getLogger(ConfigHarvesterInterceptor.class);
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
                    filterField(configs, k);
                }
            });

            //we need to add own serializers which are not here if we intercept consumer or are not string if we are in 
            //KafkaStreams app
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            createTopic(this.topicName, 1, (short) 1, props);
            Producer<String, Object> producer = getProducer(props);
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
     * check if config contains {@link ConfigHarvesterInterceptor#TOPIC_NAME} attribute, assign it as target topic and
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
     * Replace value by given key k with masked string
     *
     * @param <T>
     * @param configs
     * @param k
     */
    private <T> void filterField(Map<String, T> configs, String k) {
        configs.replace(k, (T) "XXXX-XXX-XXX-XXX");
    }

    /**
     * Not used as we need only catch configuration properties during app initialzation. Just return unchanged record
     *
     * @param record
     * @return unchanged record
     */
    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        return record;
    }

    /**
     * Not used as we need only catch configuration properties during app initialzation
     *
     * @param metadata
     * @param exception
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        //noop
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
