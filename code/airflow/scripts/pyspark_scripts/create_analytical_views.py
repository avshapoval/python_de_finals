import argparse
from pyspark.sql import SparkSession


def create_user_activity_view(src_tgt_url: str, src_tgt_driver: str, tgt_mart_name: str):
    """
    Создает/перезаписывает витрину активности пользователей с разбивкой по статусу заказа.

    Args:
        src_tgt_url (str): URL для подключения к источнику и приемнику.
        src_tgt_driver (str): Драйвер для подключения к СУБД.
        tgt_mart_name (str): Имя целевой витрины данных.
    """
    # Создание Spark сессии
    spark = (
        SparkSession.builder
        .appName(f"Create_mart_{tgt_mart_name}")
        .getOrCreate()
    )

    # Чтение данных из "STG" слоя
    users_df = ( 
        spark.read
        .format("jdbc")
        .option("url", src_tgt_url)
        .option("driver", src_tgt_driver)
        .option("dbtable", "users") \
        .load()
    )

    orders_df = (
        spark.read
        .format("jdbc")
        .option("url", src_tgt_url)
        .option("driver", src_tgt_driver)
        .option("dbtable", "orders")
        .load()
    )

    # Создание витрины активности пользователей с разбивкой по статусу заказа
    user_activity_df = (
        orders_df.join(users_df, "user_id")
        .groupBy("user_id", "first_name", "last_name", "status")
        .agg({"order_id": "count", "total_amount": "sum"})
        .withColumnRenamed("count(order_id)", "order_count")
        .withColumnRenamed("sum(total_amount)", "total_spent")
    )

    # Overwrite в приемнике
    (
        user_activity_df.write
        .format("jdbc")
        .option("url", src_tgt_url)
        .option("driver", src_tgt_driver)
        .option("dbtable", f"mart_{tgt_mart_name}")
        .mode("overwrite")
        .save()
    )
    
    # Остановка Spark сессии
    spark.stop()


def create_product_sales_view(src_tgt_url: str, src_tgt_driver: str, tgt_mart_name: str):
    """
    Создает/перезаписывает витрину продаж продуктов с разбивкой по статусу заказа.
    
    Args:
        src_tgt_url (str): URL для подключения к источнику и приемнику.
        src_tgt_driver (str): Драйвер для подключения к СУБД.
        tgt_mart_name (str): Имя целевой витрины данных.
    """
    # Создание Spark сессии
    spark = (
        SparkSession.builder
        .appName(f"Create_mart_{tgt_mart_name}")
        .getOrCreate()
    )

   # Чтение данных из "STG" слоя
    products_df = ( 
        spark.read
        .format("jdbc")
        .option("url", src_tgt_url)
        .option("driver", src_tgt_driver)
        .option("dbtable", "products") \
        .load()
    )

    order_details_df = ( 
        spark.read
        .format("jdbc")
        .option("url", src_tgt_url)
        .option("driver", src_tgt_driver)
        .option("dbtable", "order_details") \
        .load()
    )

    orders_df = ( 
        spark.read
        .format("jdbc")
        .option("url", src_tgt_url)
        .option("driver", src_tgt_driver)
        .option("dbtable", "orders") \
        .load()
    )

    # Создание витрины продаж продуктов с разбивкой по статусу заказа
    product_sales_df = (
        order_details_df.join(products_df, "product_id")
        .join(orders_df, "order_id")
        .groupBy("product_id", "name", "status")
        .agg({"quantity": "sum", "total_price": "sum"})
        .withColumnRenamed("sum(quantity)", "total_quantity_sold")
        .withColumnRenamed("sum(total_price)", "total_sales")
    )

    # Overwrite в приемнике
    (
        product_sales_df.write
        .format("jdbc")
        .option("url", src_tgt_url)
        .option("driver", src_tgt_driver)
        .option("dbtable", f"mart_{tgt_mart_name}")
        .mode("overwrite")
        .save()
    )

    # Остановка Spark сессии
    spark.stop()


def main():
    """
    Точка входа. Парсит аргументы и запускает создание/перезапись соответствующей витрины.

    Args:
        --src_tgt_url (str): URL для подключения к источнику и приемнику.
        --src_tgt_driver (str): Драйвер для подключения к СУБД.
        --target_mart (str): Имя целевой витрины данных. Возможные значения: 'user_activity', 'product_sales'.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_tgt_url', type=str, required=True)
    parser.add_argument("--src_tgt_driver", type=str, required=True)
    parser.add_argument('--target_mart', type=str, required=True, choices=['user_activity', 'product_sales'])
    args = parser.parse_args()

    if args.target_mart == "user_activity":
        create_user_activity_view(args.src_tgt_url, args.src_tgt_driver, args.target_mart)

    elif args.target_mart == "product_sales":
        create_product_sales_view(args.src_tgt_url, args.src_tgt_driver, args.target_mart)


if __name__ == "__main__":
    main()