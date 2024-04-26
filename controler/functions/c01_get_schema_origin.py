import pandas as pd
import logging
import os
import sys
import json
import psycopg2
from datetime import datetime


logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(levelname)s - %(message)s")

path_txt_type_db = "/home/michel/Documentos/Projetos/etl_with_apache_beam_and_table_control/controler/functions/type_db.txt"

with open(path_txt_type_db, 'r') as tdb:
    type_db = tdb.read().strip()

if type_db == "postgres":
    path_credentiales = '/home/michel/Documentos/Projetos/etl_with_apache_beam_and_table_control/config/conect_db_postgres.json'
elif type_db == "oracle":
    path_credentiales = '/home/michel/Documentos/Projetos/etl_with_apache_beam_and_table_control/config/conect_db_oracle.json'
else:
    print(f"C01 - Não há como criar a controler no banco informado >> {type_db}")
    

titulo = 'Execute Create Control Type Postegres-SQL'

def read_schema():
    print(f'{"#" * 30} {titulo} {"#" * 30}\n')
    print(f'Date Execute: {datetime.now()}\n')
    print(f'For: Michel Souza Santana\n')
    print(f'{"_" * (60 + len(titulo) + 2)}') # Acrescenta-se a soma + 2 para compensar os espações antes e depois do título

            
    logging.info(f'Iniciando a conexão com o banco!\n')
    with open(path_credentiales) as f:
        config = json.load(f)

    # Definindo as variáveis de ambiente
    os.environ['DB_USER'] = config['DB_USER']
    os.environ['DB_PASSWORD'] = config['DB_PASSWORD']
    os.environ['DB_HOST'] = config['DB_HOST']
    os.environ['DB_PORT'] = config['DB_PORT']
    os.environ['DB_DATABASE'] = config['DB_DATABASE']
    os.environ['NOME_PASTA_SCHEMA_ORIGIN'] = config['NOME_PASTA_SCHEMA_ORIGIN']
    
    #Pegando as credenciais via variável de ambiente
    USERNAME = os.getenv('DB_USER')
    PASSWORD = os.getenv('DB_PASSWORD')
    HOST = os.getenv('DB_HOST')
    PORT = os.getenv('DB_PORT')
    DATABASE = os.getenv('DB_DATABASE')
    NOME_PASTA_SCHEMA_ORIGIN = os.getenv('NOME_PASTA_SCHEMA_ORIGIN')

    conn_params = {
        "host": HOST,
        "database": DATABASE,
        "user": USERNAME,
        "password": PASSWORD,
        "port": PORT
    }

    # Conectar ao banco de dados
    conn = psycopg2.connect(**conn_params)
    
    # Criar um cursor para executar consultas SQL
    cursor = conn.cursor()

    logging.info(f'Conexão com o banco estabelecida!\n')

    logging.info(f'Executando a quey!\n')

    #Rodando uma Query para teste
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'  
        """)
    result = cursor.fetchall()

    list_names = []
    for row in result:
        list_names.append(row[0])
    
    logging.info(f'Dados colhidos:\n{list_names}\n\n')
    # Fechar o cursor e a conexão
    cursor.close()
    conn.close()

    for table_name in list_names:

        logging.info(f'Iniciando a conexão com o banco!\n')

        with open(path_credentiales) as f:
            config = json.load(f)

        conn_params = {
        "host": HOST,
        "database": DATABASE,
        "user": USERNAME,
        "password": PASSWORD,
        "port": PORT
        }

        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        logging.info(f'Conexão estabelelcida para obter schema da tabela {table_name}!\n')

        query = f"""SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}' 
                AND table_schema = 'public'"""
        
        cursor.execute(query)

        col_names = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        sql_df = pd.DataFrame(rows, columns=col_names)

        sql_df.to_csv(f'{NOME_PASTA_SCHEMA_ORIGIN}/schemas-{table_name}.csv')

        logging.info(f'Dados salvos!\n')