package no.sysco.middleware.kafka.interceptor.zipkin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Before;
import org.junit.Test;
import zipkin2.reporter.Sender;
import zipkin2.reporter.kafka11.KafkaSender;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class AbstractTracingInterceptorTest {

  private Map<String, String> initialConfig = new HashMap<>();

  @Before
  public void setUp() {
    initialConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
  }

  @Test
  public void shouldConfigureWhenProducerDefaultConfig() {
    //Given
    initialConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-app");
    //When
    AbstractTracingInterceptor interceptor = new AbstractTracingInterceptorImpl();
    interceptor.configure(initialConfig);
    //Then
    assertNotNull(interceptor.tracing);
    assertEquals(TracingInterceptorConfig.ZIPKIN_REMOTE_SERVICE_NAME_DEFAULT, interceptor.remoteServiceName);
  }

  @Test
  public void shouldConfigureWhenConsumerDefaultConfig() {
    //Given
    initialConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-app");
    //When
    AbstractTracingInterceptor interceptor = new AbstractTracingInterceptorImpl();
    interceptor.configure(initialConfig);
    //Then
    assertNotNull(interceptor.tracing);
    assertEquals(TracingInterceptorConfig.ZIPKIN_REMOTE_SERVICE_NAME_DEFAULT, interceptor.remoteServiceName);
  }

  @Test
  public void shouldConfigureWhenLocalServiceNameProvided() {
    //Given
    initialConfig.put(TracingInterceptorConfig.ZIPKIN_LOCAL_SERVICE_NAME_CONFIG, "app");
    //When
    AbstractTracingInterceptor interceptor = new AbstractTracingInterceptorImpl();
    interceptor.configure(initialConfig);
    //Then
    assertNotNull(interceptor.tracing);
    assertEquals(TracingInterceptorConfig.ZIPKIN_REMOTE_SERVICE_NAME_DEFAULT, interceptor.remoteServiceName);
  }

  @Test
  public void shouldCreateKafkaSenderWhenNoSenderConfigProvided() {
    //Given
    //When
    AbstractTracingInterceptor interceptor = new AbstractTracingInterceptorImpl();
    Sender sender = interceptor.buildSender(initialConfig);
    //Then
    assertTrue(sender instanceof KafkaSender);
  }

  @Test
  public void shouldCreateKafkaSenderWhenZipkinKafkaConfigProvided() {
    //Given
    initialConfig.put(TracingInterceptorConfig.ZIPKIN_BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    //When
    AbstractTracingInterceptor interceptor = new AbstractTracingInterceptorImpl();
    Sender sender = interceptor.buildSender(initialConfig);
    //Then
    assertTrue(sender instanceof KafkaSender);
  }

  @Test
  public void shouldBuildUrlSenderWhenZipkinApiUrlProvided() {
    //Given
    initialConfig.put(TracingInterceptorConfig.ZIPKIN_LOCAL_SERVICE_NAME_CONFIG, "app");
    initialConfig.put(TracingInterceptorConfig.ZIPKIN_API_URL_CONFIG, "http://zipkin:9411");
    //When
    AbstractTracingInterceptor interceptor = new AbstractTracingInterceptorImpl();
    Sender sender = interceptor.buildSender(initialConfig);
    //Then
    assertTrue(sender instanceof URLConnectionSender);
  }

  private static class AbstractTracingInterceptorImpl extends AbstractTracingInterceptor {
  }

}