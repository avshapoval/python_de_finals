FROM postgres:17-bookworm

RUN apt-get update && apt-get install -y \
    gettext

COPY init_db_template.sql /sql/
COPY init_db.sh /docker-entrypoint-initdb.d/

RUN chmod +x /docker-entrypoint-initdb.d/init_db.sh