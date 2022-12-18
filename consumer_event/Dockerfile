FROM python:3.11-alpine

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY consumer_event.py /app/consumer_event.py
COPY ./db_connect_consumer.py /app/db_connect_consumer.py


ENTRYPOINT ["python", "-u", "consumer_event.py"]
