import re
import paho.mqtt.client as mqtt
import psycopg2
from datetime import datetime
import rpyc
from cryptography.fernet import Fernet, InvalidToken

timeout_conexao_bd = 2

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
    dado = msg.payload.decode()
    dado_decrypt = decrypt_msg(dado)

    if(dado_decrypt):

        #se for impar, eu faço
        #se não, verifico se o outro está on
        #se estiver on, ele faz. Caso esteja off, eu faço
        if(num_processo % 2 != 0):
            rotina_salvar_bds(dado_decrypt)
            incrementa_num_processo()

        else:
            try:
                proxy = rpyc.connect('localhost', port_rcp, config={'allow_public_attrs': True})
                if(proxy.root.ta_vivo()):
                    print('OUTRO faz')

            except ConnectionRefusedError:
                rotina_salvar_bds(dado_decrypt)
                incrementa_num_processo()

    else:
        print('Dados não reconhecidos')

    print('----------------------------')

def on_publish(client, userdata, mid):
    pass

def decrypt_msg(dado):
    chave_criptografia = 'H4IjB9PjnThC1V54d6R0r8vOB7Tdw1V1wN-MdZAZABc='.encode()
    cipher = Fernet(chave_criptografia)

    try:
        dado_decrypt = cipher.decrypt(dado).decode()

        if(verifica_padrao_crypto(dado_decrypt)):
            return dado_decrypt
        
        else:
            return False
    except InvalidToken:
        print('Token não reconhecido')

def rotina_salvar_bds(dados):
    umidade = int(dados.split(',')[0])
    data_atual = dados.split(',')[1]

    publica_umidade_atuador(umidade, data_atual)

    banco1_ativo = verifica_conexao_bd(string_connetion_bd1)
    banco2_ativo = verifica_conexao_bd(string_connetion_bd2)
    verifica_sincronismo_bds(banco1_ativo, banco2_ativo)

    salva_umidade_bd(umidade, data_atual)

def verifica_sincronismo_bds(banco1_ativo, banco2_ativo):
    
    try:
        if(banco1_ativo and banco2_ativo):
            select_query = 'SELECT MAX(id) FROM umidade_terra;'
            dados = executa_select_query_2_bancos(select_query)

            ultimo_id_bd1 = dados[0]
            ultimo_id_bd2 = dados[1]

            #significa que o bd2 está atrasado
            if(ultimo_id_bd1 > ultimo_id_bd2):
                print('Banco 2 está atrasado')
                lista_dados_perdidos = retorna_dados_perdidos(ultimo_id_bd2 + 1, ultimo_id_bd1, string_connetion_bd1)

                for dado in lista_dados_perdidos:
                    print(dado)
                    salva_retorativo_umidade_bd(string_connetion_bd2, dado)

            #significa que o bd1 está atrasado
            elif(ultimo_id_bd2 > ultimo_id_bd1):
                print('Banco 1 está atrasado')
                lista_dados_perdidos = retorna_dados_perdidos(ultimo_id_bd1 + 1, ultimo_id_bd2, string_connetion_bd2)

                for dado in lista_dados_perdidos:
                    print(dado)
                    salva_retorativo_umidade_bd(string_connetion_bd1, dado)
    except TypeError:
        pass

def retorna_dados_perdidos(id_inicio, id_fim, string_bd):
    select_query = f'SELECT data_hora, umidade, molhou FROM umidade_terra WHERE id BETWEEN {id_inicio} AND {id_fim} ORDER BY id;'
    dados = executa_select_bd_escolhido(string_bd, select_query)
    return [(data_hora.strftime("%Y-%m-%d %H:%M:%S"), umidade, molhou) for data_hora, umidade, molhou in dados]

def salva_umidade_bd(umidade, data_atual):
    molhou = True if umidade < retorna_limiar() else False

    insert_query = f"INSERT INTO umidade_terra (data_hora, umidade, molhou) VALUES ('{data_atual}', {umidade}, {molhou});"
    executa_query(insert_query)

def salva_retorativo_umidade_bd(string_bd, dado):
    insert_query = f"INSERT INTO umidade_terra (data_hora, umidade, molhou) VALUES ('{dado[0]}', {dado[1]}, {dado[2]});"
    executa_query_bd_escolhido(string_bd, insert_query)

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
    conn = None
    try:
        conn = psycopg2.connect(string_connetion_bd1, connect_timeout=timeout_conexao_bd)
        cur = conn.cursor()

        cur.execute(query)
        conn.commit()

    except psycopg2.OperationalError as e:
        print('BANCO 1 fora +++ EXECUTA QUERY')

    finally:
        if(conn):
            conn.close()

    conn2 = None
    try:
        conn2 = psycopg2.connect(string_connetion_bd2, connect_timeout=timeout_conexao_bd)
        cur2 = conn2.cursor()

        cur2.execute(query)
        conn2.commit()

    except psycopg2.OperationalError as e:
        print('BANCO 2 fora +++ EXECUTA QUERY')

    finally:
        if(conn2):
            conn2.close()

def executa_select_query(query):
    conn = None
    conn2 = None
    try:
        conn = psycopg2.connect(string_connetion_bd1, connect_timeout=timeout_conexao_bd)
        cur = conn.cursor()

        cur.execute(query)
        return cur.fetchone()[0]
    except psycopg2.OperationalError as e:
        print('BANCO 1 fora +++ SELECT QUERY')

        try:
            conn2 = psycopg2.connect(string_connetion_bd2, connect_timeout=timeout_conexao_bd)
            cur2 = conn2.cursor()
            cur2.execute(query)
            return cur2.fetchone()[0]

        except psycopg2.OperationalError as e:
            print('BANCO 2 fora +++ SELECT QUERY')

        finally:
            if(conn2):
                conn2.close()

    finally:
        if(conn):
            conn.close()

def executa_select_query_2_bancos(query):
    conn = psycopg2.connect(string_connetion_bd1, connect_timeout=timeout_conexao_bd)
    cur = conn.cursor()
    cur.execute(query)
    dados_banco1 = cur.fetchone()[0]
    conn.close()

    conn2 = psycopg2.connect(string_connetion_bd2, connect_timeout=timeout_conexao_bd)
    cur2 = conn2.cursor()
    cur2.execute(query)
    dados_banco2 = cur2.fetchone()[0]
    conn2.close()

    return [dados_banco1, dados_banco2]

def executa_select_bd_escolhido(string_bd, query):
    conn = psycopg2.connect(string_bd, connect_timeout=timeout_conexao_bd)
    cur = conn.cursor()
    cur.execute(query)
    dados = cur.fetchall()
    conn.close()

    return dados

def executa_query_bd_escolhido(string_bd, query):
    conn = psycopg2.connect(string_bd, connect_timeout=timeout_conexao_bd)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()

def verifica_conexao_bd(string_conexao):
    conn_teste = None
    try:
        conn_teste = psycopg2.connect(string_conexao, connect_timeout=timeout_conexao_bd)
        cur_teste = conn_teste.cursor()
        cur_teste.execute("SELECT 1")
        resultado = cur_teste.fetchone()
        return resultado == (1,)
    
    except Exception:
        return False
    
    finally:
        if(conn_teste):
            conn_teste.close()

def verifica_padrao_crypto(data):
    padrao = r'^\d+,\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
    correspondencia = re.match(padrao, data)
    return bool(correspondencia)

client = mqtt.Client()
client.connect(broker_mqtt, port_mqtt)
client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish
client.loop_forever()
