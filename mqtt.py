import paho.mqtt.client as mqtt
import pymongo
import json
import ast

def saveInDatabase(data_dict:dict):
    client = pymongo.MongoClient("mongodb+srv://svkm:svkm@master.zm8sy.mongodb.net/svkm?retryWrites=true&w=majority")
    db = client.svkm.logs
    db.insert(data_dict)
    client.close()

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("dataFromThing")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    # print(msg.topic+" "+(str(msg.payload)))
    msg=json.loads(msg.payload)
    saveInDatabase(msg)


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("18.188.160.180", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()