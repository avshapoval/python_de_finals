import time
import json
import random
import os
import logging
from kafka import KafkaProducer

from faker import Faker

def generate_user_msg(fake: Faker):
    """
    Генерирует случайные данные в json.

    Args:
        fake (Faker): Экземпляр Faker, используемый для генерации случайных данных.
    """
    return {
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "registration_date": fake.date_time_between(start_date='-1y', end_date='now').isoformat(),
        "loyalty_status": random.choice(["Gold", "Silver", "Bronze"])
    }
    

def main():
    """Непрерывная генерация и отправка случайных данных в топик kafka."""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Топик для записи
    kafka_bootstrap_servers = os.getenv("KAFKA_INTERNAL_CONNECT_PATH")
    kafka_topic = os.getenv("KAFKA_TOPIC_NAME")
    msg_gen_period = os.getenv("KAFKA_DATAGEN_PERIOD_SECS")

    # Продьюсер для записи в kafka
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    fake = Faker()

    msg_id = 1
    
    while True:
        msg = generate_user_msg(fake)
        producer.send(kafka_topic, value=msg)
        logger.info(f"Delivered message №{msg_id}. Sleeping for {msg_gen_period} second(s)...")
        time.sleep(float(msg_gen_period))

        msg_id += 1


if __name__ == "__main__":
    main()