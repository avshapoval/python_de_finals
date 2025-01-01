#!/bin/bash

# Создание топиков
kafka-topics.sh --create --topic $KAFKA_TOPIC_NAME --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --if-not-exists