import paho.mqtt.client as mqtt
import psycopg2
from datetime import datetime
import rpyc

# Parâmetros de conexão com o bd
dbname = 'postgres'
user = 'postgres'
password = '123456'
host = 'localhost' 
port = '5432'

conn = psycopg2.connect(f"dbname='{dbname}' user='{user}' password='{password}' host='{host}' port='{port}'")
cur = conn.cursor()

# conexão com o mqtt-server
broker="localhost"
port=1883

# limite da umidade
limiar = 50

def on_connect(client, userdata, flags, rc):
    client.subscribe("/umidade")

def on_message(client, userdata, msg):
    dados = msg.payload.decode()

    umidade = int(dados.split(',')[0])
    data_atual = dados.split(',')[1]

    equipara_dados(umidade, data_atual)
    
    if (umidade < retorna_limiar()):
        print("Enviado: " + str(umidade))
        client.publish("/molhar", umidade)

def on_publish(client, userdata, mid):
    pass

def salva_umidade_bd(umidade, data_atual):
    molhou = True if umidade < retorna_limiar() else False

    insert_query = f"INSERT INTO umidade_terra (data_hora, umidade, molhou) VALUES ('{data_atual}', {umidade}, {molhou});"
    try:
        cur.execute(insert_query)
        conn.commit()
    except psycopg2.errors.UniqueViolation as e:
        print('ERRO DUPLICACAO')
        alter_query = f"ALTER SEQUENCE umidade_terra_id_seq RESTART WITH {retorna_ultimo_id() + 1};"
        cur.execute(alter_query)
        conn.commit()

def salva_retorativo_umidade_bd(dado):
    insert_query = f"INSERT INTO umidade_terra (data_hora, umidade, molhou) VALUES ('{dado[0]}', {dado[1]}, {dado[2]});"
    cur.execute(insert_query)
    conn.commit()

def retorna_ultimo_id():
    select_query = 'SELECT MAX(id) FROM umidade_terra;'
    cur.execute(select_query)
    return cur.fetchone()[0]

def equipara_dados(umidade, data_atual):
    id_meu = retorna_ultimo_id()

    try:
        proxy = rpyc.connect('localhost', 18862, config={'allow_public_attrs': True})
        id_outro = proxy.root.retorna_ultimo_id()

        #esse processo está atrasado
        if(id_meu < id_outro):
            lista_dados_perdidos = proxy.root.retorna_dados_perdidos(id_meu + 1, id_outro)

            limiar_atual = proxy.root.retorna_limiar()
            altera_limiar(limiar_atual)

            for dado in lista_dados_perdidos:
                salva_retorativo_umidade_bd(dado)

        else:
            salva_umidade_bd(umidade, data_atual)

    except (EOFError, TypeError, ConnectionRefusedError) as e:
        print("Outro servidor fora")
        salva_umidade_bd(umidade, data_atual)

def retorna_limiar():
    select_query = 'SELECT * FROM limiar;'
    cur.execute(select_query)
    return cur.fetchone()[0]

def altera_limiar(novo_limiar):
    update_query = f"UPDATE limiar SET limiar = {novo_limiar};"
    cur.execute(update_query)
    conn.commit()

client = mqtt.Client()
client.connect(broker, port)
client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish
client.loop_forever()
