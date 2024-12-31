FROM python:3.10-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

WORKDIR /app/src/

CMD ["python", "-u", "generate_pg_data.py"]