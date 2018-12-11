package no.sysco.middleware.kafka.interceptor.zipkin;

import brave.Span;
import brave.propagation.TraceContextOrSamplingFlags;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Consumer Interceptor that creates spans when records are received from Consumer API.
 * It creates a span per Record, and link it with an incoming context that could be
 * stored in Records header.
 */
public class TracingConsumerInterceptor<K, V> extends BaseTracingInterceptor implements ConsumerInterceptor<K, V> {

  private static final String POLL_OPERATION = "on_consume";

  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
    if (records.isEmpty() || tracing.isNoop()) {
      return records;
    }
    Map<String, Span> consumerSpansForTopic = new LinkedHashMap<>();
    for (TopicPartition partition : records.partitions()) {
      String topic = partition.topic();
      List<ConsumerRecord<K, V>> recordsInPartition = records.records(partition);
      for (ConsumerRecord<K, V> record : recordsInPartition) {
        TraceContextOrSamplingFlags extracted = extractor.extract(record.headers());

        // If we extracted neither a trace context, nor request-scoped data (extra),
        // make or reuse a span for this topic
        if (extracted.samplingFlags() != null && extracted.extra().isEmpty()) {
          Span consumerSpanForTopic = consumerSpansForTopic.get(topic);
          if (consumerSpanForTopic == null) {
            consumerSpansForTopic.put(topic,
                consumerSpanForTopic = tracing.tracer().nextSpan(extracted).name("poll")
                    .kind(Span.Kind.CONSUMER)
                    .tag(KafkaTagKey.KAFKA_TOPIC, topic)
                    .tag(KafkaTagKey.KAFKA_GROUP_ID, groupId)
                    .tag(KafkaTagKey.KAFKA_CLIENT_ID, clientId)
                    .start());
          }
          // no need to remove propagation headers as we failed to extract anything
          injector.inject(consumerSpanForTopic.context(), record.headers());
        } else { // we extracted request-scoped data, so cannot share a consumer span.
          Span span = tracing.tracer().nextSpan(extracted);
          if (!span.isNoop()) {
            span.name(POLL_OPERATION).kind(Span.Kind.CONSUMER)
                .tag(KafkaTagKey.KAFKA_TOPIC, topic)
                .tag(KafkaTagKey.KAFKA_CLIENT_ID, clientId)
                .tag(KafkaTagKey.KAFKA_GROUP_ID, groupId);
            if (remoteServiceName != null) {
              span.remoteServiceName(remoteServiceName);
            }
            span.start().finish(); // span won't be shared by other records
          }
          // remove prior propagation headers from the record
          tracing.propagation().keys().forEach(key -> record.headers().remove(key));
          injector.inject(span.context(), record.headers());
        }
      }
    }
    consumerSpansForTopic.values().forEach(span -> {
      if (remoteServiceName != null) {
        span.remoteServiceName(remoteServiceName);
      }
      span.finish();
      LOGGER.debug("Consumer Record intercepted: {}", span.context());
    });
    return records;
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
    //Do nothing
  }

  @Override
  public void close() {
    tracing.close();
  }
}
