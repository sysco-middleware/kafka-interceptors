.PHONY: all
all: build

.PHONY: build
build:
	mvn clean package

.PHONY: dc-up
docker:
	docker-compose -f docker-compose.yml -f docker-compose-zipkin.yml up -d

.PHONY: dc-connector-up
dc-connector-up: build
	docker-compose -f docker-compose.yml -f docker-compose-zipkin.yml -f docker-compose-connectors.yml up -d

.PHONY: dc-ksql-up
dc-ksql-up: build
	docker-compose -f docker-compose.yml -f docker-compose-zipkin.yml -f docker-compose-ksql.yml up -d

.PHONY: dc-down
dc-down:
	docker-compose down --remove-orphans

.PHONY: deploy-source-connector
deploy-source-connector:
	curl -XPUT -H 'Content-Type:application/json' -d @examples/jdbc-source.json http://localhost:8083/connectors/jdbc_source/config

.PHONY: deploy-sink-connector
deploy-sink-connector:
	curl -XPUT -H 'Content-Type:application/json' -d @examples/jdbc-sink.json http://localhost:8084/connectors/jdbc_sink/config
