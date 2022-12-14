import json
import db_connect_events
import uvicorn
from fastapi import FastAPI
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['kafka:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))


app = FastAPI()

def events_create():
    sql_createEvents = '''CREATE TABLE IF NOT EXISTS Events (
        id SERIAL PRIMARY KEY,
        type varchar(255),
        team_1 varchar(255),
        team_2 varchar(255),
        event_date varchar(255),
        score int2,
        state varchar(50)
        CONSTRAINT check_state
        CHECK(state IN('created', 'active', 'finished'))); '''
    db_connect_events.cursor.execute(sql_createEvents)
    print("Table Events Created")


events_create()

@app.get("/events")
def get_events():
    query = "SELECT * FROM events"
    try:
        db_connect_events.cursor.execute(query)
        return db_connect_events.cursor.fetchall()
    except:
        return "Cannot get all events"


@app.get("/event")
def get_events(id):
    query = 'SELECT * FROM events WHERE id = {}'.format(id)
    try:
        db_connect_events.cursor.execute(query)
        return db_connect_events.cursor.fetchall()
    except:
        return "Cannot fetch user with id"


@app.delete("/event")
def delete_events(id):
    query = "DELETE FROM events WHERE id = {}".format(id)
    try:
        db_connect_events.cursor.execute(query)
        return "Deleted event with id {}".format(id)
    except:
        return "Cannot delete user with id"

@app.put("/event")
def update_events(id, val):
    res = dict(zip(id.split(), val.split()))
    # res = json.dumps(res, default=lambda o: '2')
    producer.send('events.taxonomy', res)
    producer.flush()
    return res

    # except:
    #     return "Cannot update event with id"

@app.post("/event")
def create_events(col, val):
    try:
        res = dict(zip(col.split(), val.split()))
        # res2 = json.dumps(res, default=lambda o: '2')
        producer.send('events.taxonomy', res)
        producer.flush()
        return res
    except:
         return "Something went wrong when creating new event"


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8001)
