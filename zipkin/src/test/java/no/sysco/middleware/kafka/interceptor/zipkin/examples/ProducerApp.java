package no.sysco.middleware.kafka.interceptor.zipkin.examples;

import no.sysco.middleware.kafka.interceptor.zipkin.TracingProducerInterceptor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerApp {
  public static void main(String[] args) {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Collections.singletonList(
        TracingProducerInterceptor.class));
    Producer<String, String> producer = new KafkaProducer<>(producerConfig);
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", "1", "abc");
    try {
      producer.send(producerRecord).get();
      Thread.sleep(1000);
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }
}
