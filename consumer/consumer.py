import db_connect_consumer as db
from json import loads
from kafka import KafkaConsumer



Event_Writer = KafkaConsumer(bootstrap_servers=['kafka:9092'],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id='my-group',
                             value_deserializer=lambda x: loads(x.decode('utf-8')))
Event_Writer.subscribe(topics=['events.taxonomy'])

sql_insert = 'INSERT INTO Events ({}) VALUES ({});'
for msg in Event_Writer:
    d = msg.value.values()
    res = dict(*d)
    # print(*res)
    # print(msg.value)
    if 'create' in msg.value:
        db.cursor.execute(sql_insert.format(*res.keys(), *res.values()))
    elif 'update' in msg.value:
        print('updated')
    else:
        print('gg')






