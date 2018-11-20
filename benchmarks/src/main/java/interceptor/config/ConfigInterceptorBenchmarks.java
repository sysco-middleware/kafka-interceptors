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
package interceptor.config;

import java.util.concurrent.TimeUnit;
import no.sysco.middleware.kafka.interceptor.config.ConfigCollectorProducerInterceptor;
import no.sysco.middleware.kafka.interceptor.config.ConfigCollectorConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 *
 * @author Sysco Middleware AS
 */
@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(value = 3, jvmArgsAppend = "-Djmh.stack.lines=3")
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class ConfigInterceptorBenchmarks {

    private ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topic", "key", "value");
    private ConsumerRecords<Object, Object> consumerRecords = ConsumerRecords.EMPTY;

    private ConfigCollectorProducerInterceptor<String, String> pInterceptor = new ConfigCollectorProducerInterceptor<>();
    private ConfigCollectorConsumerInterceptor<Object, Object> cInterceptor = new ConfigCollectorConsumerInterceptor<>();

    @Benchmark
    public ProducerRecord<String, String> interceptEmptyOnSend() {
        return pInterceptor.onSend(producerRecord);
    }

    @Benchmark
    public ConsumerRecords interceptEmptyOnConsume() {
        return cInterceptor.onConsume(consumerRecords);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .addProfiler("gc")
                .addProfiler("stack")
                .include(".*" + ConfigInterceptorBenchmarks.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

}
