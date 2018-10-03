package no.sysco.middleware.kafka.interceptor.zipkin;

import brave.Tracing;
import brave.internal.Nullable;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.sampler.Sampler;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.Sender;
import zipkin2.reporter.kafka11.KafkaSender;
import zipkin2.reporter.urlconnection.URLConnectionSender;

/**
 * Configure Interceptor tools to create Spans and send it to Zipkin.
 */
abstract class AbstractTracingInterceptor implements Configurable {
  static final Logger LOGGER = LoggerFactory.getLogger(AbstractTracingInterceptor.class);

  Tracing tracing;
  Injector<Headers> injector;
  Extractor<Headers> extractor;
  @Nullable
  String remoteServiceName;

  @Override
  public void configure(Map<String, ?> map) {
    // Creating sender
    final Sender sender = buildSender(map);
    // Create Reporter
    final String zipkinServiceName =
        extractString(map, TracingInterceptorConfig.ZIPKIN_LOCAL_SERVICE_NAME_CONFIG);
    final String kafkaGroupId = extractString(map, ConsumerConfig.GROUP_ID_CONFIG);
    final String kafkaClientId = extractString(map, ProducerConfig.CLIENT_ID_CONFIG);
    final String localServiceName;
    if (zipkinServiceName == null || zipkinServiceName.trim().isEmpty()) {
      if (kafkaGroupId == null || kafkaGroupId.trim().isEmpty()) {
        localServiceName = kafkaClientId;
      } else {
        localServiceName = kafkaGroupId;
      }
    } else {
      localServiceName = zipkinServiceName;
    }
    final String zipkinRemoteServiceName =
        extractString(map, TracingInterceptorConfig.ZIPKIN_REMOTE_SERVICE_NAME_CONFIG);
    if (zipkinRemoteServiceName == null || zipkinRemoteServiceName.trim().isEmpty()) {
      remoteServiceName = TracingInterceptorConfig.ZIPKIN_REMOTE_SERVICE_NAME_DEFAULT;
    } else {
      remoteServiceName = zipkinRemoteServiceName;
    }
    final Reporter<Span> reporter = AsyncReporter.create(sender);
    //Create Interceptor tools
    tracing =
        Tracing.newBuilder()
            .localServiceName(localServiceName)
            .sampler(Sampler.ALWAYS_SAMPLE)
            .spanReporter(reporter)
            .build();
    injector = tracing.propagation().injector(KafkaPropagation.HEADER_SETTER);
    extractor = tracing.propagation().extractor(KafkaPropagation.HEADER_GETTER);

    LOGGER.info("Brave Interceptor configured");
  }

  Sender buildSender(Map<String, ?> map) {
    final String zipkinApiUrl = extractString(map, TracingInterceptorConfig.ZIPKIN_API_URL_CONFIG);
    final String zipkinBootstrapServers =
        extractString(map, TracingInterceptorConfig.ZIPKIN_BOOTSTRAP_SERVERS_CONFIG);
    final Sender sender;
    if (zipkinApiUrl == null || zipkinApiUrl.trim().isEmpty()) {
      if (zipkinBootstrapServers == null || zipkinBootstrapServers.trim().isEmpty()) {
        final String kafkaBootstrapServers =
            extractString(map, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        if (kafkaBootstrapServers != null) {
          sender = KafkaSender.create(kafkaBootstrapServers).toBuilder().build();
          LOGGER.info("Brave Interceptor: Kafka sender created with Bootstrap servers: {}",
              kafkaBootstrapServers);
        } else {
          final String kafkaBootstrapServersList =
              extractList(map, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
          sender = KafkaSender.create(kafkaBootstrapServersList).toBuilder().build();
          LOGGER.info("Brave Interceptor: Kafka sender created with Bootstrap servers: {}",
              kafkaBootstrapServers);
        }
      } else {
        sender = KafkaSender.create(zipkinBootstrapServers).toBuilder().build();
        LOGGER.info("Brave Interceptor: Kafka sender created with Bootstrap servers: {}", zipkinBootstrapServers);
      }
    } else {
      sender = URLConnectionSender.create(zipkinApiUrl).toBuilder().build();
      LOGGER.info("Brave Interceptor: URL connection sender created with URL: {}", zipkinApiUrl);
    }
    return sender;
  }

  private String extractList(Map<String, ?> map, String key) {
    final String value;
    final Object valueObject = map.get(key);
    if (valueObject != null) {
      AbstractList valueList = (AbstractList) valueObject;
      value = String.join(",", valueList);
    } else {
      LOGGER.warn("{} of type ArrayList is not found in properties", key);
      value = null;
    }
    return value;
  }

  String extractString(Map<String, ?> map, String key) {
    final String value;
    final Object valueObject = map.get(key);
    if (valueObject != null && valueObject instanceof String) {
      value = (String) valueObject;
    } else {
      LOGGER.warn("{} of type String is not found in properties", key);
      value = null;
    }
    return value;
  }
}
