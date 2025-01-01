#!/bin/bash

# Ожидание доступности Kafka
until kafka-topics --bootstrap-server $KAFKA_INTERNAL_CONNECT_PATH --list; do
  echo "Waiting for Kafka to be reachable..."
  sleep 5
done

echo -e 'Creating kafka topics'
kafka-topics --bootstrap-server $KAFKA_INTERNAL_CONNECT_PATH --create --if-not-exists --topic $KAFKA_TOPIC_NAME --replication-factor 1 --partitions 1

echo -e 'Successfully created the following topics:'
kafka-topics --bootstrap-server $KAFKA_INTERNAL_CONNECT_PATH --list

exit 0