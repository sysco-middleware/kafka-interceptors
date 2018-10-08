.PHONY: all
all: build

.PHONY: build
build:
	./mvnw clean package

.PHONY: dc-up
docker:
	docker-compose -f docker-compose.yml -f docker-compose-zipkin.yml up -d

.PHONY: dc-connector-up
dc-connector-up: build
	docker-compose -f docker-compose.yml -f docker-compose-zipkin.yml -f docker-compose-connectors.yml up -d

.PHONY: dc-down
dc-down:
	docker-compose down --remove-orphans

.PHONY: deploy-connector
deploy-connector:
	curl -XPUT -H 'Content-Type:application/json' -d @examples/jdbc-source.json http://localhost:8084/connectors/jdbc_source/config
