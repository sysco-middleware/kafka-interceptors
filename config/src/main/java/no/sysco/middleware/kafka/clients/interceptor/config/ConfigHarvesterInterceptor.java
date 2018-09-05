package no.sysco.middleware.kafka.clients.interceptor.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Kafka interceptor for harvesting and storing Producer/Consumer/KafkaStreams user provided configs
 *
 * @author 100tsa
 * @param <K>
 * @param <V>
 */
public class ConfigHarvesterInterceptor<K, V> implements ProducerInterceptor<K, V>, ConsumerInterceptor<K, V> {

    private static final String TOPIC_NAME = "__clients";
    private static final ObjectMapper MAPPER = new ObjectMapper();
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
     * Callback interceptor function
     *
     * @param configs user provided congiguration properties
     */
    @Override
    public void configure(final Map<String, ?> configs) {

        MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        try {
            final Properties props = new Properties();
            configs.forEach((k, v) -> {
                if (null != v && !Arrays.asList(IGNORED_PROPERTIES).contains(k)) {
                    props.put(k, v);
                }
            });

            configs.forEach((k, v) -> {
                if (Arrays.asList(FILTERED_CONFIG_VALUES).contains(k)) {
                    filterHelper(configs, k);
                }
            });

            //we need to add own serializers which are not here if we intercept consumer or are not string if we are in 
            //KafkaStreams app
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            Utils.createTopic(TOPIC_NAME, 1, (short) 1, props);
            Producer<String, Object> producer = Utils.getProducer(props);
            //we send config not props so we have all original values
            String jsonizedConf = MAPPER.writeValueAsString(configs);

            producer.send(new ProducerRecord<>(TOPIC_NAME, props.getProperty("client.id"), jsonizedConf));
        } catch (JsonProcessingException ex) {
            Logger.getLogger(ConfigHarvesterInterceptor.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }

    }
    /**
     * Replace value by given key k with masked string
     * @param <T>
     * @param configs
     * @param k 
     */
    private <T> void filterHelper(Map<String, T> configs, String k) {
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
