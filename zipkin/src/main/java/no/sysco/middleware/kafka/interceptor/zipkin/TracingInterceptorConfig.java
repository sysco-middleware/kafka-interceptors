package no.sysco.middleware.kafka.interceptor.zipkin;

public class TracingInterceptorConfig {
  public static final String ZIPKIN_API_URL_CONFIG = "zipkin.api.url";

  public static final String ZIPKIN_BOOTSTRAP_SERVERS_CONFIG = "zipkin.bootstrap.servers";

  public static final String ZIPKIN_LOCAL_SERVICE_NAME_CONFIG = "zipkin.local.service_name";
  public static final String ZIPKIN_LOCAL_SERVICE_NAME_DEFAULT = "kafka-client";

  public static final String ZIPKIN_REMOTE_SERVICE_NAME_CONFIG = "zipkin.remote.service_name";
  static final String ZIPKIN_REMOTE_SERVICE_NAME_DEFAULT = "kafka";
}
