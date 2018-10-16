package com.example.interceptor.zipkin;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import no.sysco.middleware.kafka.interceptor.zipkin.TracingProducerInterceptor;
import no.sysco.middleware.kafka.interceptor.config.ProducerConfigHarvesterInterceptor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerApp {
  public static void main(String[] args) {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-app");
    producerConfig.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
        Arrays.asList(TracingProducerInterceptor.class, ProducerConfigHarvesterInterceptor.class));

    Producer<String, String> producer = new KafkaProducer<>(producerConfig);
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", "1", "abc");
    try {
      producer.send(producerRecord).get(1, TimeUnit.SECONDS);
      Thread.sleep(1000);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      e.printStackTrace();
    }
  }
}
