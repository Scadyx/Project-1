FROM python:3.11-alpine

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY consumer_bets.py /app/consumer_bets.py
COPY db_connect_consumer_bets.py /app/db_connect_consumer_bets.py


ENTRYPOINT ["python", "-u", "consumer_bets.py"]