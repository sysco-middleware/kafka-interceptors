package no.sysco.middleware.kafka.interceptor.zipkin;

import brave.Tracing;
import zipkin2.Span;

import java.util.concurrent.ConcurrentLinkedDeque;

class BaseTracingTest {
  ConcurrentLinkedDeque<Span> spans = new ConcurrentLinkedDeque<>();
  Tracing tracing =
      Tracing.newBuilder()
          .spanReporter(spans::add)
          .build();
}
