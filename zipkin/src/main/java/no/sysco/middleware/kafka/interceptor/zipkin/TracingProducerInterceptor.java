package no.sysco.middleware.kafka.interceptor.zipkin;

import brave.Span;
import brave.propagation.TraceContextOrSamplingFlags;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import zipkin2.Endpoint;

/**
 * Producer Interceptor to trace Records send to a Kafka Topic.
 * It will extract context from incoming Record, if exist injected in its header,
 * and use it to link it to the Span created by the interceptor.
 */
public class TracingProducerInterceptor<K, V> extends BaseTracingInterceptor implements ProducerInterceptor<K, V> {

  private static final String SEND_OPERATION = "on_send";

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
    TraceContextOrSamplingFlags traceContextOrSamplingFlags = extractor.extract(record.headers());
    Span span = tracing.tracer().nextSpan(traceContextOrSamplingFlags);
    tracing.propagation().keys().forEach(key -> record.headers().remove(key));
    injector.inject(span.context(), record.headers());
    if (!span.isNoop()) {
      if (record.key() instanceof String && !"".equals(record.key())) {
        span.tag(KafkaTagKey.KAFKA_KEY, record.key().toString());
      }
      if (remoteServiceName != null) {
        span.remoteEndpoint(Endpoint.newBuilder().serviceName(remoteServiceName).build());
      }
      span.tag(KafkaTagKey.KAFKA_TOPIC, record.topic())
          .tag(KafkaTagKey.KAFKA_CLIENT_ID, clientId)
          .name(SEND_OPERATION)
          .kind(Span.Kind.PRODUCER)
          .start();
    }
    span.finish();
    LOGGER.debug("Producer Record intercepted: {}", span.context());
    return record;
  }

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception exception) {
    //Do nothing
  }

  @Override
  public void close() {
    tracing.close();
  }

}
