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

import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

/**
 *
 * @author 100tsa
 */
public class ConfigHarvesterInterceptorIT {

    private static KafkaContainer kafka;

    private static final int WARMUP_ITERATIONS = 1_000_000;

    private static final int TEST_ITERATIONS = 1_000_000;

    private final Properties defaultProducerProps;

    private static int kafkaPort;

    @BeforeClass
    public static void setUpClass() {
        ConfigHarvesterInterceptorIT.kafka = new KafkaContainer();
        ConfigHarvesterInterceptorIT.kafka.start();
        ConfigHarvesterInterceptorIT.kafkaPort = ConfigHarvesterInterceptorIT.kafka.getFirstMappedPort();
    }

    @AfterClass
    public static void tearDownClass() {
        ConfigHarvesterInterceptorIT.kafka.stop();
    }

    public ConfigHarvesterInterceptorIT() {
        System.out.println("Create properties for Kafka running on port " + kafkaPort);
        this.defaultProducerProps = new Properties();
        this.defaultProducerProps.put("bootstrap.servers", "127.0.0.1:" + kafkaPort);
        this.defaultProducerProps.put("acks", "all");
        this.defaultProducerProps.put("retries", 0);
        this.defaultProducerProps.put("batch.size", 16384);
        this.defaultProducerProps.put("linger.ms", 1);
        this.defaultProducerProps.put("buffer.memory", 33554432);
        this.defaultProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.defaultProducerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    @Test
    public void testAddedInterceptorLatencyWithProducerCreation() {

        Properties props = new Properties();
        props.putAll(this.defaultProducerProps);
        System.out.println("Sending warmup requests");
        //warm up kafka instance with some requests
        createProducerAndSendRequests(WARMUP_ITERATIONS, props);

        //send data without interceptor and measure time
        System.out.println("Sending requests without interceptor");

        long startRunWithoutInterceptorNs = System.nanoTime();
        createProducerAndSendRequests(TEST_ITERATIONS, props);
        long elapsedWithoutInterceptorNs = System.nanoTime() - startRunWithoutInterceptorNs;

        System.out.println("\tCompleted in " + elapsedWithoutInterceptorNs / 1_000_000 + " ms");

        //now add interceptor and test elapsed time with this one
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "no.sysco.middleware.kafka.interceptor.config.ConfigHarvesterInterceptor");
        System.out.println("Sending requests with interceptor");

        long startRunWithInterceptor = System.nanoTime();
        createProducerAndSendRequests(TEST_ITERATIONS, props);
        long elapsedWithInterceptorNs = System.nanoTime() - startRunWithInterceptor;

        System.out.println("\tCompleted in " + elapsedWithInterceptorNs / 1_000_000 + " ms");
        //allow up to 1 sec diff (send config data to server and acknowledge)
        if (elapsedWithoutInterceptorNs < elapsedWithInterceptorNs) {
            Assert.assertEquals(elapsedWithoutInterceptorNs, elapsedWithInterceptorNs, 1000_000_000);
        } else {
            Assert.assertTrue(elapsedWithInterceptorNs < elapsedWithoutInterceptorNs);
        }

    }

    @Test
    public void testAddedInterceptorLatencyWithoutProducerCreation() {
        Properties props = new Properties();
        props.putAll(this.defaultProducerProps);
        System.out.println("Sending warmup requests");

        long elapsedWithoutInterceptorNs;
        //send data without interceptor and measure time
        try (Producer<String, Object> producerWithoutInterceptor = ConfigHarvesterInterceptor.getProducer(props)) {
            //send data without interceptor and measure time
            System.out.println("Sending requests without interceptor");
            long startRunWithoutInterceptorNs = System.nanoTime();
            sendRequests(TEST_ITERATIONS, producerWithoutInterceptor);
            elapsedWithoutInterceptorNs = System.nanoTime() - startRunWithoutInterceptorNs;
        }
        System.out.println("\tCompleted in " + elapsedWithoutInterceptorNs / 1_000_000 + " ms");

        //now add interceptor and test elapsed time with this one
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "no.sysco.middleware.kafka.interceptor.config.ConfigHarvesterInterceptor");
        System.out.println("Sending requests with interceptor");

        long startRunWithInterceptor = System.nanoTime();
        Producer<String, Object> producerWithInterceptor = ConfigHarvesterInterceptor.getProducer(props);
        sendRequests(TEST_ITERATIONS, producerWithInterceptor);
        long elapsedWithInterceptorNs = System.nanoTime() - startRunWithInterceptor;

        System.out.println("\tCompleted in " + elapsedWithInterceptorNs / 1_000_000 + " ms");
        //max diff 100 ms if without interceptor takes less time than with

        if (elapsedWithoutInterceptorNs < elapsedWithInterceptorNs) {
            Assert.assertEquals(elapsedWithoutInterceptorNs, elapsedWithInterceptorNs, 100_000_000);
        } else {
            Assert.assertTrue(elapsedWithInterceptorNs < elapsedWithoutInterceptorNs);
        }
    }

    /**
     *
     *
     * @param iterations
     */
    private void createProducerAndSendRequests(int iterations, Properties props) {
        try (Producer<String, Object> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < iterations; i++) {
                Future<RecordMetadata> metadata = producer.send(new ProducerRecord<>("test-topic", "test-string-data"));
            }
        }
    }

    /**
     *
     *
     * @param iterations
     */
    private void sendRequests(int iterations, Producer producer) {
        for (int i = 0; i < iterations; i++) {
            Future<RecordMetadata> metadata = producer.send(new ProducerRecord<>("test-topic", "test-string-data"));
        }
    }

}
