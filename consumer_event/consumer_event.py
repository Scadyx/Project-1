import db_connect_consumer as db
from json import loads
from kafka import KafkaConsumer



Event_Writer = KafkaConsumer(bootstrap_servers=['kafka:9092'],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id='event',
                             value_deserializer=lambda x: loads(x.decode('utf-8')))
Event_Writer.subscribe(topics=['events.taxonomy'])

for msg in Event_Writer:
    unp = dict(*msg.value.values())
    col = ','.join(unp.keys())
    val = ','.join(unp.values())
    if 'create' in msg.value:
        db.cursor.execute(f'INSERT INTO Events ({col}) VALUES ({val});')
    elif 'update' in msg.value:
        dt = []
        for key, value in unp.items():
            if key not in 'id':
                dt.append(f"{key} = {value}")
        gen = (','.join(dt))
        print(gen)
        db.cursor.execute(f'UPDATE Events SET {gen} WHERE id = {unp["id"]}')

    else:
        print('gg')







