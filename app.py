from flask import Flask, request, jsonify
import psycopg2
import paho.mqtt.client as mqtt
import json
import threading

app = Flask(__name__)

conn = psycopg2.connect(
    host="localhost",
    database="fire_monitoring",
    user="your_user",
    password="your_password"
)
cursor = conn.cursor()

def on_message(client, userdata, message):
    payload = json.loads(message.payload.decode("utf-8"))
    temperature = payload["temperature"]
    humidity = payload["humidity"]
    smoke = payload["smoke"]
    cursor.execute("INSERT INTO sensor_data (temperature, humidity, smoke) VALUES (%s, %s, %s)", (temperature, humidity, smoke))
    conn.commit()
    print(f"Data Inserted: Temperature: {temperature}, Humidity: {humidity}, Smoke: {smoke}")

client = mqtt.Client()
client.on_message = on_message
client.connect("broker.hivemq.com", 1883, 60)
client.subscribe("forest_fire/data")

def run_mqtt():
    client.loop_forever()

threading.Thread(target=run_mqtt).start()

@app.route('/data', methods=['GET'])
def get_data():
    cursor.execute("SELECT * FROM sensor_data ORDER BY timestamp DESC LIMIT 10")
    data = cursor.fetchall()
    for row in data:
        print(f"Temperature: {row[1]}, Humidity: {row[2]}, Smoke: {row[3]}, Timestamp: {row[4]}")
    return jsonify(data)

@app.route('/alerts', methods=['GET'])
def get_alerts():
    cursor.execute("SELECT * FROM sensor_data WHERE temperature > 35 OR smoke = 1 ORDER BY timestamp DESC LIMIT 10")
    alerts = cursor.fetchall()
    for row in alerts:
        print(f"ALERT - Temperature: {row[1]}, Humidity: {row[2]}, Smoke: {row[3]}, Timestamp: {row[4]}")
    return jsonify(alerts)

if __name__ == '__main__':
    app.run(debug=True)
