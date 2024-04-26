import os
import pandas as pd
import logging
import json
import sys

path_txt_type_db = "/home/michel/Documentos/Projetos/etl_with_apache_beam_and_table_control/controler/functions/type_db.txt"

with open(path_txt_type_db , 'r') as tdb:
    type_db = tdb.read().strip()

if type_db == "postgres":
    path_credentiales = 'config/conect_db_postgres.json'
elif type_db == "oracle":
    path_credentiales = 'config/conect_db_oracle.json'
else:
    print(f"Não há como criar a controler no banco informado >> {type_db}")
    sys.exit()

def union(): 
    with open(path_credentiales) as f:
            config = json.load(f)

            # Definindo as variáveis de ambiente
            os.environ['NOME_PASTA_CONTROL_TABLE'] = config['NOME_PASTA_CONTROL_TABLE']
            os.environ['NOME_PASTA_SCHEMA_CONTROLER'] = config['NOME_PASTA_SCHEMA_CONTROLER']
            os.environ['NOME_PASTA_SCHEMA_ORIGIN'] = config['NOME_PASTA_SCHEMA_ORIGIN']
            
            #Pegando as credenciais via variável de ambiente
            NOME_PASTA_CONTROL_TABLE = os.getenv('NOME_PASTA_CONTROL_TABLE')
            NOME_PASTA_SCHEMA_CONTROLER = os.getenv('NOME_PASTA_SCHEMA_CONTROLER')
            NOME_PASTA_SCHEMA_ORIGIN = os.getenv('NOME_PASTA_SCHEMA_ORIGIN')

    logging.info(f'Iniciando separação do tipos das colunas do banco!\n')

    list_shemas_tables = os.listdir(NOME_PASTA_SCHEMA_ORIGIN)
    for table in list_shemas_tables:
        new = pd.read_csv(f'{NOME_PASTA_SCHEMA_ORIGIN}/{table}')
        new = new.iloc[:, -1]  # Selecionando apenas a última coluna
        if os.path.exists(f'{NOME_PASTA_SCHEMA_CONTROLER}/schema_controler.csv'):
            old = pd.read_csv(f'{NOME_PASTA_SCHEMA_CONTROLER}/schema_controler.csv')
            old = old.iloc[:, -1]  # Selecionando apenas a última coluna
            new_df = pd.concat([old, new]).drop_duplicates()
            new_df.to_csv(f'{NOME_PASTA_SCHEMA_CONTROLER}/schema_controler.csv')
            logging.info(f'Schema {table} salvo!\n')
        else:
            new_df = new.drop_duplicates() 
            new_df.to_csv(f'{NOME_PASTA_SCHEMA_CONTROLER}/schema_controler.csv')  
            logging.info(f'Schema {table} salvo!\n')



