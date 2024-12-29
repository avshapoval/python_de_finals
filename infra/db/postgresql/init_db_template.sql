-- БД, юзер и гранты
CREATE DATABASE ${POSTGRESQL_APP_DB};
CREATE USER ${POSTGRESQL_APP_USER} WITH PASSWORD '${POSTGRESQL_APP_PASSWORD}';
GRANT ALL PRIVILEGES ON DATABASE ${POSTGRESQL_APP_DB} TO ${POSTGRESQL_APP_USER};

-- Подключение к БД
\c ${POSTGRESQL_APP_DB}

-- Создание схемы, изменение дефолтной схемы и гранты
CREATE SCHEMA ${POSTGRESQL_APP_SCHEMA} AUTHORIZATION ${POSTGRESQL_APP_USER};
SET search_path TO ${POSTGRESQL_APP_SCHEMA};

ALTER DEFAULT PRIVILEGES IN SCHEMA ${POSTGRESQL_APP_SCHEMA} GRANT ALL PRIVILEGES ON TABLES TO ${POSTGRESQL_APP_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA ${POSTGRESQL_APP_SCHEMA} GRANT USAGE ON SEQUENCES TO ${POSTGRESQL_APP_USER};

-- Пользователи
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    first_name VARCHAR(200),
    last_name VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(200),
    registration_date TIMESTAMP,
    loyalty_status VARCHAR(20)
);

-- Категории товаров
CREATE TABLE IF NOT EXISTS product_categories (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(200),
    parent_category_id INT
);

-- Товары
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(200),
    description TEXT,
    category_id INT,
    price DECIMAL(10, 2),
    stock_quantity INT,
    creation_date TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES product_categories(category_id)
);

-- Заказы
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    user_id INT,
    order_date TIMESTAMP,
    total_amount DECIMAL(10, 2),
    status VARCHAR(20),
    delivery_date TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Детали заказов
CREATE TABLE IF NOT EXISTS order_details (
    order_detail_id SERIAL PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    price_per_unit DECIMAL(10, 2),
    total_price DECIMAL(10, 2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);