FROM confluentinc/cp-kafka:latest

COPY create_kafka_topics.sh /usr/bin/create_kafka_topics.sh
RUN chmod +x /usr/bin/create_kafka_topics.sh

CMD ["sh", "-c", "create_kafka_topics.sh & /etc/confluent/docker/run"]