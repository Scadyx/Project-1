FROM python:3.11-alpine

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY ./data.jsonl /app/data.jsonl
COPY ./main.py /app/main.py
COPY ./db_connect.py /app/db_connect.py

ENTRYPOINT ["python", "main.py"]
