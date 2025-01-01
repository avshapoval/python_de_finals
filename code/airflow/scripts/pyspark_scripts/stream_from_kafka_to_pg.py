import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType


def stream_kafka_to_postgres(kafka_bootstrap_servers: str, kafka_topic: str, target_url: str, target_driver: str, target_table: str):
    """
    Читает данные из Kafka и записывает их в PostgreSQL.

    Args:
        kafka_bootstrap_servers (str): Адреса Kafka bootstrap servers.
        kafka_topic (str): Название Kafka топика.
        target_url (str): URL для подключения к приемнику.
        target_driver (str): Драйвер для подключения к приемнику.
        target_table (str): Имя таблицы в приемнике.
    """
    # Определение схемы данных
    schema = StructType([
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("registration_date", TimestampType(), True),
        StructField("loyalty_status", StringType(), True)
    ])

    # Создание Spark сессии
    spark = (
        SparkSession.builder
        .appName("KafkaToPG")
        .getOrCreate()
    )

    # Чтение данных из Kafka
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .load()
    )

    # Преобразование данных
    df = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    # Запись данных в PostgreSQL
    query = (
        df.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: 
            batch_df.write
            .format("jdbc")
            .option("url", target_url)
            .option("driver", target_driver)
            .option("dbtable", target_table)
            .mode("append")
            .save()
        )
        .start()
    )

    # Ожидаем прерывания, предполагается, что стриминговое приложение не должно останавливаться
    query.awaitTermination()
    spark.stop()


def main():
    """
    Точка входа. Парсит аргументы и запускает стриминг данных из Kafka в СУБД.

    Args:
        --kafka_bootstrap_servers (str): Адреса Kafka bootstrap servers.
        --kafka_topic (str): Название Kafka топика.
        --target_url (str): URL для подключения к приемнику.
        --target_driver (str): Драйвер для подключения к приемнику.
        --target_table (str): Имя таблицы в приемнике.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka_bootstrap_servers", type=str, required=True)
    parser.add_argument("--kafka_topic", type=str, required=True)
    parser.add_argument("--target_url", type=str, required=True)
    parser.add_argument("--target_driver", type=str, required=True)
    parser.add_argument("--target_table", type=str, required=True)
    
    args = parser.parse_args()
    stream_kafka_to_postgres(args.kafka_bootstrap_servers, args.kafka_topic, args.target_url, args.target_driver, args.target_table)


if __name__ == "__main__":
    main()