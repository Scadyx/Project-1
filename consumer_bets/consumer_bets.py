import db_connect_consumer_bets as db
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json

query = """
SELECT
    bets.id,
    CASE
        WHEN
            split_part(score, '-', 1)::integer < split_part(score, '-', 2)::integer and market = 'team_2'
            OR split_part(score, '-', 1)::integer > split_part(score, '-', 2)::integer and market = 'team_1'
            OR split_part(score, '-', 1)::integer = split_part(score, '-', 2)::integer and market = 'draw'
        THEN
            CASE
                WHEN events.state = 'active' THEN 'winning'
                ELSE 'win'
            END
        ELSE
            CASE
                WHEN events.state = 'active' THEN 'losing'
                ELSE 'lose'
            END
    END AS state
FROM bets
JOIN events
    ON 1 = 1
    AND eventId = events.id
    AND eventId = {}
    AND events.state = 'active'
    OR events.state = 'finished'
;"""


producer = KafkaProducer(bootstrap_servers=['kafka:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

Bet_Scorer = KafkaConsumer(bootstrap_servers=['kafka:9092'],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id='bets',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
Bet_Scorer.subscribe(topics=['events.taxonomy'])

for msg in Bet_Scorer:
    unp = dict(*msg.value.values())
    event_id = unp["id"]
    db.cursor.execute(query.format(event_id))
    res = db.cursor.fetchall()
    for line in res:
        bet_id, bet_state = line
        producer.send("bets.state", {'id': bet_id, 'state': bet_state})
        producer.flush()

