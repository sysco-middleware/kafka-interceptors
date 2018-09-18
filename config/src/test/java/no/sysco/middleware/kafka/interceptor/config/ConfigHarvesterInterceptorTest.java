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

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Sysco Middleware AS
 */
public class ConfigHarvesterInterceptorTest {
    
    public ConfigHarvesterInterceptorTest() {
    }

    /**
     * Test of onSend method, of class ConfigHarvesterInterceptor.
     */
    @Test
    public void testOnSend() {
        ConfigHarvesterInterceptor instance = new ConfigHarvesterInterceptor();
        ProducerRecord expResult = new ProducerRecord<>("test-topic", "test-key", "test-value");
        ProducerRecord result = instance.onSend(expResult);
        assertEquals(expResult, result);
    }

    /**
     * Test of onConsume method, of class ConfigHarvesterInterceptor.
     */
    @Test
    public void testOnConsume() {
        ConfigHarvesterInterceptor instance = new ConfigHarvesterInterceptor();
        ConsumerRecords consumedRecords = instance.onConsume(ConsumerRecords.EMPTY);
        assertEquals(consumedRecords, ConsumerRecords.empty());
    }
}
