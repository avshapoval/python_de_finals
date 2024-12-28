import random
from datetime import datetime, timedelta
from typing import Callable

from faker import Faker
from faker_commerce import Provider as CommerceProvider
from psycopg2.extensions import cursor

from database import PGConnector


class DataGenerator:
    """Класс для генерации данных в СУБД"""

    def __init__(self, db_connector: PGConnector):
        """
        Инициализация DataGenerator.

        Args:
            db_connector (PGConnector): Объект для управления соединениями с СУБД.
        """
        self.db_connector = db_connector
        self.fake = Faker()
        self.fake.add_provider(CommerceProvider)

        self.user_ids = []
        self.product_ids = []
        self.order_ids = []
        self.category_ids = []

    def generate_users(self, cur: cursor, num_users: int = 100) -> None:
        """Генерация данных для таблицы users.

        Args:
            cur (cursor): Объект курсора для выполнения SQL-запросов.
            num_users (int): Количество пользователей для генерации.
        """
        for _ in range(num_users):
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()

            email = self.fake.email()
            phone = self.fake.phone_number()

            registration_date = self.fake.date_time_between(start_date='-1y', end_date='now')
            loyalty_status = random.choice(["Gold", "Silver", "Bronze"])

            cur.execute(
                "INSERT INTO users (first_name, last_name, email, phone, registration_date, loyalty_status) VALUES (%s, %s, %s, %s, %s, %s) RETURNING user_id",
                (first_name, last_name, email, phone, registration_date, loyalty_status)
            )
            user_id = cur.fetchone()[0]
            self.user_ids.append(user_id)

    def generate_product_categories(self, cur: cursor, num_categories: int = 10) -> None:
        """Генерация данных для таблицы product_categories.

        Args:
            cur (cursor): Объект курсора для выполнения SQL-запросов.
            num_categories (int): Количество случайных категорий товаров для генерации. 10 основных создаются всегда.
        """
        predefined_categories = ["Electronics", "Clothing", "Books", "Home", "Toys", "Sport", "Cars", "Beauty", "Health", "Food"]
        
        for category in predefined_categories:
            cur.execute(
                "INSERT INTO product_categories (name, parent_category_id) VALUES (%s, %s) RETURNING category_id",
                (category, None)
            )
            category_id = cur.fetchone()[0]
            self.category_ids.append(category_id)

        for _ in range(num_categories):
            name = self.fake.words(nb=1)

            # 50/50, что получится подкатегория
            parent_category_id = random.choice(self.category_ids) if self.category_ids and random.choice([True, False]) else None

            cur.execute(
                "INSERT INTO product_categories (name, parent_category_id) VALUES (%s, %s) RETURNING category_id",
                (name, parent_category_id)
            )
            category_id = cur.fetchone()[0]
            self.category_ids.append(category_id)

    def generate_products(self, cur: cursor, num_products: int = 100) -> None:
        """Генерация данных для таблицы products.

        Args:
            cur (cursor): Объект курсора для выполнения SQL-запросов.
            num_products (int): Количество товаров для генерации.
        """
        for _ in range(num_products):
            name = self.fake.ecommerce_name()
            description = self.fake.paragraph(nb_sentences=2)

            category_id = random.randint(1, 10)
            price = round(random.uniform(10.0, 100.0), 2)

            stock_quantity = random.randint(1, 100)
            creation_date = datetime.now() - timedelta(days=random.randint(1, 365))
            
            cur.execute(
                "INSERT INTO products (name, description, category_id, price, stock_quantity, creation_date) VALUES (%s, %s, %s, %s, %s, %s) RETURNING product_id",
                (name, description, category_id, price, stock_quantity, creation_date)
            )
            product_id = cur.fetchone()[0]
            self.product_ids.append(product_id)

    def generate_orders(self, cur: cursor, num_orders: int = 100) -> None:
        """Генерация данных для таблицы orders.

        Args:
            cur (cursor): Объект курсора для выполнения SQL-запросов.
            num_orders (int): Количество заказов для генерации.
        """
        for _ in range(num_orders):
            user_id = random.choice(self.user_ids)
            order_date = self.fake.date_time_between(start_date='-1y', end_date='now')
            
            total_amount = round(random.uniform(50.0, 500.0), 2)
            status = random.choice(["Pending", "Completed"])
            delivery_date = order_date + timedelta(days=random.randint(1, 10))
            
            cur.execute(
                "INSERT INTO orders (user_id, order_date, total_amount, status, delivery_date) VALUES (%s, %s, %s, %s, %s) RETURNING order_id",
                (user_id, order_date, total_amount, status, delivery_date)
            )
            order_id = cur.fetchone()[0]
            self.order_ids.append(order_id)

    def generate_order_details(self, cur: cursor, num_order_details: int = 100) -> None:
        """Генерация данных для таблицы order_details.

        Args:
            cur (cursor): Объект курсора для выполнения SQL-запросов.
            num_order_details (int): Количество деталей заказов для генерации.
        """
        for _ in range(num_order_details):
            order_id = random.choice(self.order_ids)
            product_id = random.choice(self.product_ids)

            quantity = random.randint(1, 10)
            price_per_unit = round(random.uniform(10.0, 100.0), 2)
            total_price = round(price_per_unit * quantity, 2)

            cur.execute(
                "INSERT INTO order_details (order_id, product_id, quantity, price_per_unit, total_price) VALUES (%s, %s, %s, %s, %s)",
                (order_id, product_id, quantity, price_per_unit, total_price)
            )

    def execute_generation_task(self, cur: cursor, task_name: str, generate_method: Callable, num_elements: int = 100) -> None:
        """Выполнение задачи генерации

        Args:
            progress (Progress): rich.progress для логирования прогресса
            cur (cursor): Курсор к СУБД
            task_name (str): Название таски
            num_elements (int, optional): Количество элементов, которые нужно сгенерировать. Defaults to 100.
        """
        print(f"Generating data ({num_elements} rows) for {task_name}...")
        generate_method(cur, num_elements)
        print(f"Generated {num_elements} rows")

    def orchestrate_population(self) -> None:
        """Оркестрация процесса генерации данных для всех таблиц."""
        
        with (
            self.db_connector.get_conn() as conn,
            conn.cursor() as cur
        ):
            # Юзеры
            self.execute_generation_task(cur, 'users', self.generate_users, 5000)

            # Категории
            self.execute_generation_task(cur, 'product categories', self.generate_product_categories, 500)

            # Продукты
            self.execute_generation_task(cur, 'products', self.generate_products, 10000)

            # Заказы
            self.execute_generation_task(cur, 'orders', self.generate_orders, 40000)

            # Детали заказов
            self.execute_generation_task(cur, 'order details', self.generate_order_details, 40000)

            # Все или ничего
            conn.commit()

            print("Everything seems fine, session commited")


def main():
    """Основная функция для генерации данных."""
    pg_connector = PGConnector()
    data_generator = DataGenerator(pg_connector)

    try:
        data_generator.orchestrate_population()
    except Exception as e:
        print(f"Error generating data: {e}")


if __name__ == "__main__":
    main()