package no.sysco.middleware.kafka.interceptor.zipkin;

import brave.Tracing;
import brave.internal.Nullable;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.sampler.Sampler;
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

import java.util.Map;

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
    final Reporter<Span> reporter = buildReporter(map);
    final String localServiceName = buildServiceName(map);
    tracing =
        Tracing.newBuilder()
            .localServiceName(localServiceName)
            .sampler(Sampler.ALWAYS_SAMPLE)
            .spanReporter(reporter)
            .build();
    injector = tracing.propagation().injector(KafkaPropagation.HEADER_SETTER);
    extractor = tracing.propagation().extractor(KafkaPropagation.HEADER_GETTER);

    LOGGER.info("Zipkin Interceptor configured: {}", localServiceName);
  }

  private String buildServiceName(Map<String, ?> map) {
    final String zipkinServiceName =
        (String) map.get(TracingInterceptorConfig.ZIPKIN_LOCAL_SERVICE_NAME_CONFIG);
    final String kafkaGroupId = (String) map.get(ConsumerConfig.GROUP_ID_CONFIG);
    final String kafkaClientId = (String) map.get(ProducerConfig.CLIENT_ID_CONFIG);
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
    return localServiceName;
  }

  private Reporter<Span> buildReporter(Map<String, ?> map){
    final Sender sender = buildSender(map);
    final String zipkinRemoteServiceName =
        (String) map.get(TracingInterceptorConfig.ZIPKIN_REMOTE_SERVICE_NAME_CONFIG);
    if (zipkinRemoteServiceName == null || zipkinRemoteServiceName.trim().isEmpty()) {
      remoteServiceName = TracingInterceptorConfig.ZIPKIN_REMOTE_SERVICE_NAME_DEFAULT;
    } else {
      remoteServiceName = zipkinRemoteServiceName;
    }
    return AsyncReporter.create(sender);
  }

  Sender buildSender(Map<String, ?> map) {
    final String zipkinApiUrl =
        (String) map.get(TracingInterceptorConfig.ZIPKIN_API_URL_CONFIG);
    final String zipkinBootstrapServers =
        (String) map.get(TracingInterceptorConfig.ZIPKIN_BOOTSTRAP_SERVERS_CONFIG);
    final String kafkaBootstrapServers =
        (String) map.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    final Sender sender;
    if (zipkinApiUrl == null || zipkinApiUrl.trim().isEmpty()) {
      if (zipkinBootstrapServers == null || zipkinBootstrapServers.trim().isEmpty()) {
        sender = KafkaSender.create(kafkaBootstrapServers).toBuilder().build();
        LOGGER.info("Zipkin Interceptor: Kafka sender created with Bootstrap servers: {}",
            kafkaBootstrapServers);
      } else {
        sender = KafkaSender.create(zipkinBootstrapServers).toBuilder().build();
        LOGGER.info("Zipkin Interceptor: Kafka sender created with Bootstrap servers: {}",
            zipkinBootstrapServers);
      }
    } else {
      sender = URLConnectionSender.create(zipkinApiUrl).toBuilder().build();
      LOGGER.info("Zipkin Interceptor: URL connection sender created with URL: {}", zipkinApiUrl);
    }
    return sender;
  }
}
