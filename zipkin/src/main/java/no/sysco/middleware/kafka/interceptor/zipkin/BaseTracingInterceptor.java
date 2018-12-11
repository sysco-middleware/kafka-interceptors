package no.sysco.middleware.kafka.interceptor.zipkin;

import brave.Tracing;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.sampler.Sampler;
import java.util.AbstractList;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
abstract class BaseTracingInterceptor implements Configurable {
  static final Logger LOGGER = LoggerFactory.getLogger(BaseTracingInterceptor.class);

  Tracing tracing;
  Injector<Headers> injector;
  Extractor<Headers> extractor;
  String remoteServiceName = TracingInterceptorConfig.ZIPKIN_REMOTE_SERVICE_NAME_DEFAULT;
  String clientId;
  String groupId;

  @Override
  public void configure(Map<String, ?> map) {
    groupId = extractString(map, ConsumerConfig.GROUP_ID_CONFIG);
    clientId = extractString(map, ProducerConfig.CLIENT_ID_CONFIG);
    boolean traceId128bitEnabled = Optional.ofNullable(extractString(map, TracingInterceptorConfig.ZIPKIN_TRACE_ID_128BIT_ENABLED_CONFIG))
                                            .map(Boolean::valueOf).orElse(TracingInterceptorConfig.ZIPKIN_TRACE_ID_128BIT_ENABLED_DEFAULT);
    final Reporter<Span> reporter = buildReporter(map);
    final String localServiceName = getLocalServiceName(map);
    final Float samplerRate = getSamplerRate(map);
    final Tracing.Builder tracingBuilder =
        Tracing.newBuilder()
            .localServiceName(localServiceName)
                .traceId128Bit(traceId128bitEnabled)
                .sampler(Sampler.create(samplerRate));
    if (reporter != null) {
      tracingBuilder.spanReporter(reporter);
    }
    tracing = tracingBuilder.build();
    injector = tracing.propagation().injector(KafkaPropagation.HEADER_SETTER);
    extractor = tracing.propagation().extractor(KafkaPropagation.HEADER_GETTER);
    remoteServiceName = getRemoteServiceName(map);

    LOGGER.info("Zipkin Interceptor configured with local service name {} and " +
        "remote service name {}", localServiceName, remoteServiceName);
  }

  private Float getSamplerRate(Map<String, ?> map) {
    final String rate = extractString(map, TracingInterceptorConfig.ZIPKIN_SAMPLER_RATE_CONFIG);
    if (Objects.isNull(rate)) {
      return TracingInterceptorConfig.ZIPKIN_SAMPLER_RATE_DEFAULT;
    } else {
      try {
        final Float samplerRate = Float.valueOf(rate);
        if (samplerRate > 1.0 || samplerRate <= 0.0 || samplerRate.isNaN()) {
          LOGGER.warn("Invalid sampler rate {}, must be between 0 and 1. Falling back to {}",
              samplerRate, TracingInterceptorConfig.ZIPKIN_SAMPLER_RATE_FALLBACK);
          return TracingInterceptorConfig.ZIPKIN_SAMPLER_RATE_FALLBACK;
        } else {
          return samplerRate;
        }
      } catch (NumberFormatException e) {
        LOGGER.warn(
            "Invalid sampler rate {}, must be a valid number between 0 and 1. Falling back to {}",
            rate, TracingInterceptorConfig.ZIPKIN_SAMPLER_RATE_FALLBACK);
        return TracingInterceptorConfig.ZIPKIN_SAMPLER_RATE_FALLBACK;
      }
    }
  }

  private String getRemoteServiceName(Map<String, ?> map) {
    final String zipkinRemoteServiceName =
        extractString(map, TracingInterceptorConfig.ZIPKIN_REMOTE_SERVICE_NAME_CONFIG);
    final String remoteServiceName;
    if (Objects.isNull(zipkinRemoteServiceName)) {
      remoteServiceName = TracingInterceptorConfig.ZIPKIN_REMOTE_SERVICE_NAME_DEFAULT;
    } else {
      remoteServiceName = zipkinRemoteServiceName;
    }
    return remoteServiceName;
  }

  private String getLocalServiceName(Map<String, ?> map) {
    final String localServiceName;
    final String zipkinServiceName =
        extractString(map, TracingInterceptorConfig.ZIPKIN_LOCAL_SERVICE_NAME_CONFIG);

    if (!Objects.isNull(zipkinServiceName)) {
      localServiceName = zipkinServiceName;
    } else {

      final String kafkaServiceName;
      if (Objects.isNull(groupId) || groupId.trim().isEmpty()) {
        kafkaServiceName = clientId;
      } else {
        kafkaServiceName = groupId;
      }

      if (Objects.isNull(kafkaServiceName)) {
        localServiceName = TracingInterceptorConfig.ZIPKIN_LOCAL_SERVICE_NAME_DEFAULT;
      } else {
        localServiceName = kafkaServiceName;
      }
    }
    return localServiceName;
  }

  private Reporter<Span> buildReporter(Map<String, ?> map) {
    final Sender sender = buildSender(map);
    if (Objects.isNull(sender)) {
      return null;
    }
    return AsyncReporter.create(sender);
  }

  Sender buildSender(Map<String, ?> map) {
    final String zipkinApiUrl =
        extractString(map, TracingInterceptorConfig.ZIPKIN_API_URL_CONFIG);
    final String zipkinBootstrapServers =
        extractString(map, TracingInterceptorConfig.ZIPKIN_BOOTSTRAP_SERVERS_CONFIG);
    final Sender sender;
    if (Objects.isNull(zipkinApiUrl)) {
      if (Objects.isNull(zipkinBootstrapServers)) {
        final String kafkaBootstrapServers =
            extractString(map, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        if (Objects.nonNull(kafkaBootstrapServers)) {
          sender = KafkaSender.newBuilder().bootstrapServers(kafkaBootstrapServers).build();
          LOGGER.info("Zipkin Interceptor: Kafka sender created with Bootstrap servers: {}",
              kafkaBootstrapServers);
        } else {
          final String kafkaBootstrapServersList =
              extractList(map, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
          if (kafkaBootstrapServersList != null) {
            sender = KafkaSender.newBuilder().bootstrapServers(kafkaBootstrapServersList).build();
            LOGGER.info("Brave Interceptor: Kafka sender created with Bootstrap servers: {}",
                kafkaBootstrapServersList);
          } else {
            sender = null;
            LOGGER.warn(
                "Zipkin Interceptor: No sender has been defined, spans will not be reported.");
          }
        }
      } else {
        sender = KafkaSender.newBuilder().bootstrapServers(zipkinBootstrapServers).build();
        LOGGER.info("Zipkin Interceptor: Kafka sender created with Bootstrap servers: {}",
            zipkinBootstrapServers);
      }
    } else {
      sender = URLConnectionSender.create(zipkinApiUrl);
      LOGGER.info("Zipkin Interceptor: URL connection sender created with URL: {}", zipkinApiUrl);
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

  private String extractString(Map<String, ?> map, String key) {
    final String value;
    final Object valueObject = map.get(key);
    if (valueObject instanceof String) {
      value = (String) valueObject;
    } else {
      LOGGER.warn("{} of type String is not found in properties", key);
      value = null;
    }
    return value;
  }
}
