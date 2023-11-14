import paho.mqtt.client as mqtt

broker="localhost"
port=1883

def on_connect(client, userdata, flags, rc):
    client.subscribe("/molhar")

def on_message(client, userdata, msg):
    global dados_atuais
    
    dados = msg.payload.decode()

    umidade = dados.split(',')[0]
    data_atual = dados.split(',')[1]

    print('-------------------')
    print('Molhando...')
    print(f'Umidade: {umidade} | Data: {data_atual}')

client = mqtt.Client()
client.connect(broker, port)
client.on_connect = on_connect
client.on_message = on_message
client.loop_forever()
