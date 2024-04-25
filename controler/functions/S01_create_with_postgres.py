from functions.C01_get_schema_origin import read_schema
from functions.C02_schema_union import union
from functions.C03_control_tabel_create import create_table_control
import json
import os

with open('controler/functions/type_db.txt', 'r') as tdb:
    type_db = tdb.read().strip()

path_type_db = 'config/type_db.json'

with open(path_type_db) as t:
    config = json.load(t)

    os.environ['TYPE_DB_POSTGRES'] = config['TYPE_DB_POSTGRES']
    os.environ['TYPE_DB_ORACLE'] = config['TYPE_DB_ORACLE']

    TYPE_DB_POSTGRES = os.getenv('TYPE_DB_POSTGRES')
    TYPE_DB_ORACLE = os.getenv('TYPE_DB_ORACLE')

if TYPE_DB_POSTGRES == type_db:
    path_credentiales = 'config/conect_db_postgres.json'
elif TYPE_DB_ORACLE == type_db:
    path_credentiales = 'config/conect_db_oracle.json'
else:
    print(f'Não há conecção para | {type_db} |')
    exit()

def create_table_control_types_postegres():
    # Cria o scopo para armazenar as informações colhidas no banco
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

    # Executa cada passo do processo de criação da controler type 
    read_schema() # Ler o schema de todas as tableas no banco
    union() # Concatena os types e dropa as duplicadas
    create_table_control() # Cria um csv com duas colunas sendo uma com os types em SQL e outra em Python