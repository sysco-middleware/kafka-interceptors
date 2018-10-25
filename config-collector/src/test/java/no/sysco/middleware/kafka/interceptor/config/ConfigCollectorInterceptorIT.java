package no.sysco.middleware.kafka.interceptor.config;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import no.sysco.middleware.kafka.interceptor.config.proto.ClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;

public class ConfigCollectorInterceptorIT {

    @ClassRule
    public static KafkaJunitRule kafka = new KafkaJunitRule(EphemeralKafkaBroker.create());

    @Test
    public void should_send_config_record_when_producing_and_consuming() throws ExecutionException, InterruptedException {
        final Properties producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ConfigCollectorProducerInterceptor.class.getName());
        final KafkaProducer<String, String> producer = kafka.helper().createStringProducer(producerConfig);

        final Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConfigCollectorConsumerInterceptor.class.getName());
        final KafkaConsumer<String, String> consumer = kafka.helper().createStringConsumer(consumerConfig);

        kafka.helper().produce("test_topic", producer, Collections.singletonMap("key", "value"));
        kafka.helper().consume("test_topic", consumer, 1);
        ListenableFuture<List<ConsumerRecord<byte[], byte[]>>> maybeRecords =
                kafka.helper().consume(
                        ConfigCollectorInterceptorConfig.CONFIG_COLLECTOR_TOPIC_DEFAULT,
                        kafka.helper().createByteConsumer(),
                        2);
        maybeRecords.get().forEach(record -> {
            try {
                final ClientConfig clientConfig = ClientConfig.parseFrom(record.value());
                assertNotNull(clientConfig);
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        });
    }
}
