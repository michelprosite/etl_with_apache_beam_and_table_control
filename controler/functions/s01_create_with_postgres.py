from functions.c01_get_schema_origin import read_schema
from functions.c02_schema_union import union
from functions.c03_control_tabel_create import create_table_control
import json
import os

path_credentiales = 'config/conect_db_postgres.json'

def create_table_control_types_postgres():
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

    if not os.path.exists(NOME_PASTA_CONTROL_TABLE):
        os.makedirs(NOME_PASTA_CONTROL_TABLE)
    if not os.path.exists(NOME_PASTA_SCHEMA_CONTROLER):
        os.makedirs(NOME_PASTA_SCHEMA_CONTROLER)
    if not os.path.exists(NOME_PASTA_SCHEMA_ORIGIN):
        os.makedirs(NOME_PASTA_SCHEMA_ORIGIN)
        
    read_schema()
    union()
    create_table_control()