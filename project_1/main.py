
import json
from fastapi import FastAPI
import uvicorn
import db_connect

def init_db(file_path: str):
    sql_users = '''CREATE TABLE IF NOT EXISTS Users (
                   id SERIAL PRIMARY KEY,
                   name varchar(255),
                   last_name varchar(255),
                   time_created int,
                   gender varchar(255),
                   age varchar(255),
                   city varchar(255),
                   birth_day varchar(255),
                   premium BOOL,
                   ip varchar(255),
                   balance float4)
                   ;'''


    sql_insert = 'INSERT INTO users ({}) VALUES ({}) ON CONFLICT DO NOTHING'
    db_connect.cursor.execute(sql_users)
    with open(file_path, 'r') as f:
        data = list(map(json.loads, f))
        for user in data:
            n_user = {k: v for k, v in user.items() if v is not None}
            res_keys = str(list(n_user.keys())).strip("[]").replace("'", '')
            res_val = str(list(n_user.values())).strip("[]")
            db_connect.cursor.execute(sql_insert.format(res_keys, res_val))
        print('Inserted')


def create_tables():
    sql_createBets = '''CREATE TABLE IF NOT EXISTS Bets (
    id SERIAL PRIMARY KEY,
    date_created int,
    userId int REFERENCES users(id),
    eventId int REFERENCES events(id),
    market varchar(50)
    CONSTRAINT check_market
    CHECK(market IN('team_1', 'team_2', 'draw')),
    state varchar(50)
    CONSTRAINT check_state
    CHECK(state IN('none', 'winning', 'losing', 'win', 'lose')));'''
    db_connect.cursor.execute(sql_createBets)
    print("Table Bets Created")


file = 'data.jsonl'
init_db(file)
app = FastAPI()
create_tables()


@app.get("/users")
def get_users():
    query = "SELECT row_to_json(users) FROM users"
    try:
        db_connect.cursor.execute(query)
        return db_connect.cursor.fetchall()
    except:
        return "Cannot fetch all users"

@app.get("/user")
def get_users(id):
    query = 'SELECT * FROM users WHERE id = {}'.format(id)
    try:
        db_connect.cursor.execute(query)
        return db_connect.cursor.fetchall()
    except:
        return "Cannot get user"



@app.delete("/user")
def delete_user(id):
    query = "DELETE FROM users WHERE id = {}".format(id)
    try:
        db_connect.cursor.execute(query)
        return "Deleted user with id {}".format(id)
    except:
        return "Cannot delete user with id"


@app.put("/user")
def update_user(id, val):
    query = "UPDATE users SET {} WHERE id = {}".format(val, id)
    try:
        db_connect.cursor.execute(query)
        return "updated {} in user with id {}".format(val, id)
    except:
        return "Cannot update user with id"


@app.post("/user")
def create_user(col, val):
    query = "INSERT INTO users ({}) VALUES ({})".format(col, val)
    # try:
    db_connect.cursor.execute(query)
    return "created user {}".format(val)
    # except:
    #     return "Something went wrong when creating new user"


@app.get("/bets")
def get_bets():
    query = "SELECT * FROM bets"
    try:
        db_connect.cursor.execute(query)
        return db_connect.cursor.fetchall()
    except:
        return "Cannot get all bets"


@app.get("/bet")
def get_bet(id):
    query = 'SELECT * FROM bets WHERE id = {}'.format(id)
    try:
        db_connect.cursor.execute(query)
        return db_connect.cursor.fetchall()
    except:
        return "Cannot get bet"


@app.delete("/bet")
def delete_bet(id):
    query = "DELETE FROM bets WHERE id = {}".format(id)
    try:
        db_connect.cursor.execute(query)
        return "Deleted bet with id {}".format(id)
    except:
        return "Cannot delete bet with id"


@app.put("/bet")
def update_bet(id, val):
    query = "UPDATE bets SET {} WHERE id = {}".format(val, id)
    try:
        check = ['market', 'state']
        if val in check:
            raise Exception('You can not modify market or state')
        db_connect.cursor.execute(query)
        return "updated {} in bet with id {}".format(val, id)
    except:
        return "Cannot update bet with id"


@app.post("/bet")
def create_bet(col, val):
    query = "INSERT INTO bets ({}) VALUES  ({}) ".format(col, val)
    try:
        db_connect.cursor.execute(query)
        return "created bet {}".format(val)
    except:
        return "Something went wrong when creating new bet"


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)