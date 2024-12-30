import sys
import argparse

from pyspark.sql import SparkSession


def main(source_url: str, source_driver:str, target_url: str, target_driver: str, table: str):
    # Создание Spark сессии
    spark = (
        SparkSession.builder
        .appName(f"replicate_pg_table_{table}_to_mysql")
        .getOrCreate()
    )

    # Чтение данных из источника
    pg_df = (
        spark.read
        .format("jdbc")
        .option("driver", source_driver)
        .option("url", source_url)
        .option("dbtable", table)
        .load()
    )
        

    # Overwrite в приемнике
    (
        pg_df.write
        .format("jdbc")
        .option("driver", target_driver)
        .option("url", target_url)
        .option("dbtable", table)
        .mode("overwrite")
        .save()
    )

    # Остановка Spark сессии
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_url", type=str, required=True)
    parser.add_argument("--source_driver", type=str, required=True)
    parser.add_argument("--target_url", type=str, required=True)
    parser.add_argument("--target_driver", type=str, required=True)
    parser.add_argument("--table", type=str, required=True)
    
    args = parser.parse_args()
    
    main(args.source_url, args.source_driver, args.target_url, args.target_driver, args.table)
    