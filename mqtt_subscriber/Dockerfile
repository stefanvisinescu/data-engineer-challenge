FROM python:3.11-slim

WORKDIR /app

COPY main.py .

RUN pip install paho-mqtt psycopg2-binary

CMD ["python", "main.py"]
