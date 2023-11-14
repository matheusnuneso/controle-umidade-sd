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

string_connetion_bd1 = f"dbname='{dbname}' user='{user}' password='{password}' host='{host}' port='{port_bd1}'"
string_connetion_bd2 = f"dbname='{dbname}' user='{user}' password='{password}' host='{host}' port='{port_bd2}'"

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
        #print('EU vou fazer - ' + str(num_processo))
        rotina_salvar_bds(msg.payload.decode())
        incrementa_num_processo()

    else:
        try:
            proxy = rpyc.connect('localhost', port_rcp, config={'allow_public_attrs': True})
            if(proxy.root.ta_vivo()):
                print('OUTRO faz')

        except ConnectionRefusedError:
            #print('EU vou fazer - ' + str(num_processo))
            rotina_salvar_bds(msg.payload.decode())
            incrementa_num_processo()

    print('----------------------------')

def on_publish(client, userdata, mid):
    pass

def rotina_salvar_bds(dados):
    umidade = int(dados.split(',')[0])
    data_atual = dados.split(',')[1]

    publica_umidade_atuador(umidade, data_atual)

    salva_umidade_bd(umidade, data_atual)

def salva_umidade_bd(umidade, data_atual):
    molhou = True if umidade < retorna_limiar() else False

    insert_query = f"INSERT INTO umidade_terra (data_hora, umidade, molhou) VALUES ('{data_atual}', {umidade}, {molhou});"
    executa_query(insert_query)

def salva_retorativo_umidade_bd(dado):
    insert_query = f"INSERT INTO umidade_terra (data_hora, umidade, molhou) VALUES ('{dado[0]}', {dado[1]}, {dado[2]});"
    executa_query(insert_query)

def publica_umidade_atuador(umidade, data_atual):
    if (umidade < retorna_limiar()):
        print("Enviado: " + str(umidade))
        dados_enviar = f'{str(umidade)},{data_atual}'
        client.publish("/molhar", dados_enviar)
    else: 
        print('não é necessário publicar')

def retorna_ultimo_id():
    select_query = 'SELECT MAX(id) FROM umidade_terra;'
    ultimo_id = executa_select_query(select_query)
    return ultimo_id

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
    limiar = executa_select_query(select_query)
    return limiar

def altera_limiar(novo_limiar):
    update_query = f"UPDATE limiar SET limiar = {novo_limiar};"
    executa_query(update_query)

def retorna_num_processo():
    select_query = 'SELECT * FROM controle_processo;'
    num_processo = executa_select_query(select_query)
    return num_processo

def incrementa_num_processo():
    num_novo = retorna_num_processo() + 1
    update_query = f'UPDATE controle_processo SET num_processo = {num_novo};'
    executa_query(update_query)

def executa_query(query):
    try:
        conn = psycopg2.connect(string_connetion_bd1)
        cur = conn.cursor()

        cur.execute(query)
        conn.commit()

    except psycopg2.OperationalError as e:
        print('BANCO 1 fora')

    try:
        conn2 = psycopg2.connect(string_connetion_bd2)
        cur2 = conn2.cursor()

        cur2.execute(query)
        conn2.commit()

    except psycopg2.OperationalError as e:
        print('BANCO 2 fora')

def executa_select_query(query):
    try:
        conn = psycopg2.connect(string_connetion_bd1)
        cur = conn.cursor()

        cur.execute(query)
        return cur.fetchone()[0]
    except psycopg2.OperationalError as e:
        print('BANCO 1 fora')

        try:
            conn2 = psycopg2.connect(string_connetion_bd2)
            cur2 = conn2.cursor()
            cur2.execute(query)
            return cur2.fetchone()[0]

        except psycopg2.OperationalError as e:
            print('BANCO 2 fora')

        finally:
            if(conn2):
                conn2.close()

    finally:
        if(conn):
            conn.close()

client = mqtt.Client()
client.connect(broker_mqtt, port_mqtt)
client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish
client.loop_forever()
