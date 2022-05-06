#!/usr/bin/python3

from cmath import e
from typing import Any
import paho.mqtt.client as mqtt
import pymysql.cursors
import sys
import config

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    
    client.subscribe("DbAchter")
    print("MQTT Client Connected")    
    

def on_message(client, userdata, msg):  
    decoded = msg.payload.decode('utf8')      
    db = Any
    try:
        db = pymysql.connect(host=config.mysqlHost, user=config.mysqlUser, password=config.mysqlPassword, db=config.dbName, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        db.close()        
    except BaseException as err:
        sys.exit(f"mysql connection failed: {err=}, {type(err)=}")        
    
    cursor = db.cursor()
    
    try:
        insertRequest = "INSERT INTO balkon(level) VALUES('%s')" % (decoded)
        cursor.execute(insertRequest)
        db.commit()        
    except BaseException as err:
        sys.exit(f"unable to insert: {err=}, {type(err)=}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(username=config.mqttUser, password=config.mqttPassword)

try:
    client.connect(config.mqttBroker, config.mqttBrokerPort, 60)
except BaseException as err:
    sys.exit(f"mqtt connection failed {err=}, {type(err)=}")        

client.loop_forever()
