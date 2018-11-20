# kafka-interceptors

[![Build Status](https://travis-ci.org/sysco-middleware/kafka-interceptors.svg?branch=master)](https://travis-ci.org/sysco-middleware/kafka-interceptors)
[![Maven Central](https://img.shields.io/maven-central/v/no.sysco.middleware.kafka/kafka-interceptors.svg)](https://maven-badges.herokuapp.com/maven-central/no.sysco.middleware.kafka/kafka-interceptors)

Set of interceptors to integrate to your Kafka Clients.

## Supporter Interceptors

* [Configuration Harvester](config): send clients configurations to a Kafka Topics for further analysis.

* [Zipkin](zipkin): create Zipkin traces from Producers and Consumers.
