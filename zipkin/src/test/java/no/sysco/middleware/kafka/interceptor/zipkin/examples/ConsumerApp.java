package no.sysco.middleware.kafka.interceptor.zipkin.examples;

import no.sysco.middleware.kafka.interceptor.zipkin.TracingConsumerInterceptor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class ConsumerApp {
  public static void main(String[] args) {
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-intercept-1");
    consumerConfig.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, Collections.singletonList(
        TracingConsumerInterceptor.class));

    Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
    consumer.subscribe(Collections.singletonList("test"));

    ConsumerRecords<String, String> consumerRecords = consumer.poll(10_000);

    for (ConsumerRecord<String, String> record : consumerRecords) {
      System.out.println(record);
    }

    consumer.commitSync();

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
