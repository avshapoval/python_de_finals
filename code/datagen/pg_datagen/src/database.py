import os
from contextlib import contextmanager
from typing import Generator

from psycopg2.pool import SimpleConnectionPool
from psycopg2.extensions import cursor


class PGConnector:
    """
    Класс для управления соединениями с PG через пулы коннектов.

    Attributes:
        pool (SimpleConnectionPool | None): Пул коннектов.
    """

    def __init__(self, credentials_source: str = "ENV") -> None:
        """
        Инициализация DatabaseConnector.

        Args:
            credentials_source (str): Источник учетных данных для подключения к базе данных. Defaults to "ENV".
        """
        self.pool: SimpleConnectionPool | None = None

        # Креды подключения лежат в ENV
        if credentials_source == "ENV":
            self.__create_pool_from_env()

    def __create_pool_from_env(self) -> SimpleConnectionPool:
        """
        Создание пула соединений с базой данных из переменных окружения.

        Returns:
            SimpleConnectionPool: Пул соединений для базы данных.
        """
        self.pool = SimpleConnectionPool(
            1, 
            5,
            user=os.environ['POSTGRESQL_APP_USER'],
            password=os.environ['POSTGRESQL_APP_PASSWORD'],
            host=os.environ['POSTGRESQL_APP_HOST'],
            database=os.environ['POSTGRESQL_APP_DB'],
            options=f"-c search_path={os.environ['POSTGRESQL_APP_SCHEMA']}"
        )
        return self.pool

    @contextmanager
    def get_conn(self) -> Generator[cursor, None, None]:
        """
        Контекстный менеджер для получения коннекта из пула.

        Yields:
            conn: Соединение для работы с СУБД.
        """
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)