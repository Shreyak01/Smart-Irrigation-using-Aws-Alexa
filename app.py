from typing import Optional
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import paho.mqtt.client as mqtt

import json
from pymongo import MongoClient
from bson.json_util import dumps
from datetime import datetime, timezone
from datetime import timedelta
import time
from calendar import timegm


def query_by_range_of_dates(from_date,to_date):
    # client = MongoClient(mongo_url)
    client = MongoClient("mongodb+srv://svkm:svkm@master.zm8sy.mongodb.net/svkm?retryWrites=true&w=majority")
    # db = client.svkm.logs
    # db.insert(data_dict)
    # client.close()

    db_instance = client['svkm']

    from_date = time.strptime(from_date, "%Y-%m-%d %H:%M:%S")
    from_date_epoch_time = timegm(from_date)
    to_date = time.strptime(to_date, "%Y-%m-%d %H:%M:%S")
    to_date_epoch_time = timegm(to_date)
    criteria = {"$and": [{"time": {"$gte": from_date_epoch_time, "$lte": to_date_epoch_time}}]}
    my_dict=db_instance['logs'].find(criteria)
    # if '_id' in my_dict: del my_dict['_id']
    # my_dict.pop('_id', None)
    new_list = [{k: v for k, v in d.items() if k != '_id'} for d in my_dict]
    return json.loads(dumps( new_list ))

# phrase- Alexa trigger turn on motor

# message model coming from ifttt webhook
class Item(BaseModel):
    val: str
class DataReq(BaseModel):
    from_date: str
    to_date:str
# mqtt related stuff
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+(str(msg.payload)))
    # msg=json.loads(msg.payload)
    # saveInDatabase(msg)
# fast api declaration and cors init
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.post("/getData")
async def getData(dataReq:DataReq):
    dataReq=dataReq.dict()
    from_date=dataReq['from_date']
    to_date=dataReq['to_date']
    return query_by_range_of_dates(from_date,to_date)
# http://localhost:8000/alexa
@app.post("/alexa")
async def alexa(item:Item):
    item_dict = item.dict()
        # mqtt init
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("18.118.156.14", 1883, 60)
    # ifttt msg parsing
    if(item_dict['val']=="1"):
        # turn on motor
        client.publish("motor","1")
    elif(item_dict['val']=="0"):
        # motor turn off
        client.publish("motor","0")

    client.disconnect()

    print(item_dict)

    return item_dict
if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        reload=True,
        port=8000,
    )