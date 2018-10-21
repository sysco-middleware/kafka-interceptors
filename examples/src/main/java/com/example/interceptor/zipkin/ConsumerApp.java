package com.example.interceptor.zipkin;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import no.sysco.middleware.kafka.interceptor.zipkin.TracingConsumerInterceptor;
import no.sysco.middleware.kafka.interceptor.config.ConsumerConfigCollectorInterceptor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerApp {
  public static void main(String[] args) {
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-1");
    consumerConfig.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
        Arrays.asList(TracingConsumerInterceptor.class, ConsumerConfigCollectorInterceptor.class));

    Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
    consumer.subscribe(Collections.singletonList("test"));

    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(10));

    for (ConsumerRecord<String, String> record : consumerRecords) {
      System.out.println(record);
    }

    consumer.commitSync();

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
