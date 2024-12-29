#!/bin/bash

# Создание временной директории
mkdir -p /tmp/sql

# Подстановка ENV VARS в шаблон SQL
envsubst < /sql/init_db_template.sql > /tmp/sql/init_db.sql

# Подключение к MySQL, создание БД, юзера, выдача прав и создание таблиц
mysql -u root -p${MYSQL_ROOT_PASSWORD} < /tmp/sql/init_db.sql

# Удаление временной директории
rm -rf /tmp/sql