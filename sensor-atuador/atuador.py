import paho.mqtt.client as mqtt

broker="localhost"
port=1883

def on_connect(client, userdata, flags, rc):
    client.subscribe("/molhar")

def on_message(client, userdata, msg):
    print(msg.payload.decode())

client = mqtt.Client()
client.connect(broker, port)
client.on_connect = on_connect
client.on_message = on_message
client.loop_forever()
