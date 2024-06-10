import random
import time
import json
import paho.mqtt.client as mqtt

def generate_data():
    while True:
        temperature = round(random.uniform(20.0, 40.0), 2)
        humidity = round(random.uniform(30.0, 70.0), 2)
        smoke = random.choice([0, 1])
        data = {
            "temperature": temperature,
            "humidity": humidity,
            "smoke": smoke
        }
        yield data
        time.sleep(2)

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

client = mqtt.Client()
client.on_connect = on_connect
client.connect("broker.hivemq.com", 1883, 60)

for data in generate_data():
    client.publish("forest_fire/data", json.dumps(data))
    print("Published:", data)
