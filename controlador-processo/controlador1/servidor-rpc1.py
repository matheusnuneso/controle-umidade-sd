from rpyc.utils.server import ThreadedServer

import rpyc
import psycopg2

# Parâmetros de conexão com o bd
dbname = 'postgres'
user = 'postgres'
password = '123456'
host = 'localhost' 
port = '5432'

conn = psycopg2.connect(f"dbname='{dbname}' user='{user}' password='{password}' host='{host}' port='{port}'")
cur = conn.cursor()

class Servidor1(rpyc.Service):

    def exposed_retorna_ultimo_id(self):
        select_query = 'SELECT MAX(id) FROM umidade_terra;'
        cur.execute(select_query)

        return cur.fetchone()[0]
    
    # retorna os dados entre o id_inicio e id_fim 
    def exposed_retorna_dados_perdidos(self, id_inicio, id_fim):
        select_query = f'SELECT data_hora, umidade, molhou FROM umidade_terra WHERE id BETWEEN {id_inicio} AND {id_fim};'
        cur.execute(select_query)
        dados_perdidos = [(data_hora.strftime("%Y-%m-%d %H:%M:%S"), umidade, molhou) for data_hora, umidade, molhou in cur.fetchall()]

        return dados_perdidos

    def exposed_retorna_todos_dados(self):
        select_query = 'SELECT data_hora, umidade, molhou FROM umidade_terra;'
        cur.execute(select_query)
        dados = [(data_hora.strftime("%Y-%m-%d %H:%M:%S"), umidade, molhou) for data_hora, umidade, molhou in cur.fetchall()]
        return dados

    def exposed_altera_limiar(self, novo_limiar):
        update_query = f"UPDATE limiar SET limiar = {novo_limiar};"
        cur.execute(update_query)
        conn.commit()

    def exposed_retorna_limiar(self):
        select_query = 'SELECT * FROM limiar;'
        cur.execute(select_query)
        return cur.fetchone()[0]
    
    def exposed_ta_vivo(self):
        return True

t = ThreadedServer(Servidor1, port=18861)
t.start()