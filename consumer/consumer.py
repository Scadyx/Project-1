from json import loads
from kafka import KafkaConsumer


Event_Writer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id='my-group',
                             value_deserializer=lambda x: loads(x.decode('utf-8')))
Event_Writer.subscribe(topics=['events.taxonomy'])

for msg in Event_Writer:
    print(msg)

