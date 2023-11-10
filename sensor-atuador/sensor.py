import paho.mqtt.client as paho
import random
import time
from datetime import datetime

broker="localhost"
port=1883

def on_publish(client, userdata, mid):
    print("Enviado")
    pass

client = paho.Client("admin")
client.on_publish = on_publish
client.connect(broker, port)

while(True):
    delay = random.randint(1, 3)
    umidade = str(random.randint(0, 100))
    data_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    data = umidade + ',' + data_atual

    client.publish("/umidade", data)
    print(data)
    time.sleep(delay)
