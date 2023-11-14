import paho.mqtt.client as mqtt
import psycopg2
from datetime import datetime
import rpyc

# Parâmetros de conexão com o bd
dbname = 'postgres'
user = 'postgres'
password = '123456'
host = 'localhost' 
port_bd1 = '5432'
port_bd2 = '5433'

conn = psycopg2.connect(f"dbname='{dbname}' user='{user}' password='{password}' host='{host}' port='{port_bd1}'")
cur = conn.cursor()

conn2 = psycopg2.connect(f"dbname='{dbname}' user='{user}' password='{password}' host='{host}' port='{port_bd2}'")
cur2 = conn2.cursor()

# conexão com o mqtt-server
broker_mqtt="localhost"
port_mqtt=1883

port_rcp = 18862

def on_connect(client, userdata, flags, rc):
    client.subscribe("/umidade")

def on_message(client, userdata, msg):
    num_processo = int(retorna_num_processo())

    #se for impar, eu faço
    #se não, verifico se o outro está on
    #se estiver on, ele faz. Caso esteja off, eu faço
    if(num_processo % 2 != 0):
        print('EU vou fazer - ' + str(num_processo))
        incrementa_num_processo()

    else:
        try:
            proxy = rpyc.connect('localhost', port_rcp, config={'allow_public_attrs': True})
            if(proxy.root.ta_vivo()):
                print('OUTRO faz')

        except ConnectionRefusedError:
            print('EU vou fazer - ' + str(num_processo))
            incrementa_num_processo()


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
        proxy = rpyc.connect('localhost', port_rcp, config={'allow_public_attrs': True})
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

def retorna_num_processo():
    select_query = 'SELECT * FROM controle_processo;'
    cur.execute(select_query)
    return cur.fetchone()[0]

def incrementa_num_processo():
    num_novo = retorna_num_processo() + 1

    update_query = f'UPDATE controle_processo SET num_processo = {num_novo};'

    cur.execute(update_query)
    conn.commit()

    cur2.execute(update_query)
    conn2.commit()


client = mqtt.Client()
client.connect(broker_mqtt, port_mqtt)
client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish
client.loop_forever()
