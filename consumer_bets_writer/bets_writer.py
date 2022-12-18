from kafka import KafkaConsumer
import json


Bet_Writer = KafkaConsumer(bootstrap_servers=['kafka:9092'],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id='bestie',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
Bet_Writer.subscribe(topics=['bets.state'])

for msg in Bet_Writer:
    print(msg)