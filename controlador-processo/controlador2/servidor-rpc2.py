from rpyc.utils.server import ThreadedServer

import rpyc
import psycopg2

timeout_conexao_bd = 1

# Parâmetros de conexão com o bd
dbname = 'postgres'
user = 'postgres'
password = '123456'
host = 'localhost' 
port_bd1 = '5432'
port_bd2 = '5433'

string_connetion_bd1 = f"dbname='{dbname}' user='{user}' password='{password}' host='{host}' port='{port_bd1}' connect_timeout='{timeout_conexao_bd}'"
string_connetion_bd2 = f"dbname='{dbname}' user='{user}' password='{password}' host='{host}' port='{port_bd2}' connect_timeout='{timeout_conexao_bd}'"

class Servidor1(rpyc.Service):

    def exposed_retorna_todos_dados(self):
        select_query = 'SELECT data_hora, umidade, molhou FROM umidade_terra ORDER BY data_hora DESC;'

        conn = None
        conn2 = None

        dados = False

        try:
            conn = psycopg2.connect(string_connetion_bd1)
            cur = conn.cursor()
            cur.execute(select_query)
            dados = [(data_hora.strftime("%Y-%m-%d %H:%M:%S"), umidade, molhou) for data_hora, umidade, molhou in cur.fetchall()]

        except psycopg2.OperationalError:
            print('BANCO 1 fora')

            try:
                conn2 = psycopg2.connect(string_connetion_bd2)
                cur2 = conn2.cursor()
                cur2.execute(select_query)
                dados = [(data_hora.strftime("%Y-%m-%d %H:%M:%S"), umidade, molhou) for data_hora, umidade, molhou in cur.fetchall()]

            except psycopg2.OperationalError:
                print('BANCO 2 fora')

            finally:
                if(conn2):
                    conn2.close()
        
        finally:
            if(conn):
                conn.close()

        return dados

    def exposed_altera_limiar(self, novo_limiar):
        query = f"UPDATE limiar SET limiar = {novo_limiar};"
        conn = None
        try:
            conn = psycopg2.connect(string_connetion_bd1, connect_timeout=timeout_conexao_bd)
            cur = conn.cursor()

            cur.execute(query)
            conn.commit()

        except psycopg2.OperationalError as e:
            print('BANCO 1 fora')

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
            print('BANCO 2 fora')

        finally:
            if(conn2):
                conn2.close()
    
    def exposed_ta_vivo(self):
        return True
    
    def executa_query(query):
        conn = None
        try:
            conn = psycopg2.connect(string_connetion_bd1, connect_timeout=timeout_conexao_bd)
            cur = conn.cursor()

            cur.execute(query)
            conn.commit()

        except psycopg2.OperationalError as e:
            print('BANCO 1 fora')

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
            print('BANCO 2 fora')

        finally:
            if(conn2):
                conn2.close()

t = ThreadedServer(Servidor1, port=18862)
t.start()