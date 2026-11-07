FROM docker.io/library/python:3.12-slim
WORKDIR /app
COPY src/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/producer/config.py src/producer/sse_kafka_producer.py ./
USER nobody
CMD ["python", "-u", "sse_kafka_producer.py"]
