import time
import json
import random
import os
from kafka import KafkaProducer
from faker import Faker

# TODO: 1. Допилить сервис
# 2. Допилить кафку
# 3. Приделать оркестрацию
# 4. Проверить работоспособность


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
        "registration_date": fake.date_time_between(start_date='-1y', end_date='now'),
        "loyalty_status": random.choice(["Gold", "Silver", "Bronze"])
    }
    

def main():
    """Непрерывная генерация и отправка случайных данных в топик kafka."""
    # Продьюсер для записи в kafka
    producer = KafkaProducer(bootstrap_servers='kafka:29092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    # Период между сообщениями
    period_seconds = 1

    # Топик для записи
    kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

    while True:
        msg = generate_user_msg(producer)
        producer.send(kafka_topic, value=msg)
        time.sleep(period_seconds)


if __name__ == "__main__":
    main()