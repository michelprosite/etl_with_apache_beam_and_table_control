import pandas as pd 
import logging
import json
import os

path_credentiales = 'config/conect_db_postgres.json'

def create_table_control():
    with open(path_credentiales) as f:
            config = json.load(f)

            # Definindo as variáveis de ambiente
            os.environ['NOME_PASTA_CONTROL_TABLE'] = config['NOME_PASTA_CONTROL_TABLE']
            os.environ['NOME_PASTA_SCHEMA_CONTROLER'] = config['NOME_PASTA_SCHEMA_CONTROLER']
            
            #Pegando as credenciais via variável de ambiente
            NOME_PASTA_CONTROL_TABLE = os.getenv('NOME_PASTA_CONTROL_TABLE')
            NOME_PASTA_SCHEMA_CONTROLER = os.getenv('NOME_PASTA_SCHEMA_CONTROLER')

    # Dicionário de mapeamento dos tipos SQL para os tipos Python correspondentes
    sql_to_python_types = {
        'CHAR': 'str',
        'VARCHAR2': 'str',
        'NCHAR': 'str',
        'NVARCHAR2': 'str',
        'CLOB': 'str',
        'NCLOB': 'str',
        'LONG': 'str',
        'RAW': 'bytes',
        'LONG RAW': 'bytes',
        'BLOB': 'bytes',
        'BFILE': 'str',  # Pode ser tratado como uma string
        'NUMBER': 'int',  # Ou 'float' dependendo do contexto
        'FLOAT': 'float',
        'BINARY_FLOAT': 'float',
        'BINARY_DOUBLE': 'float',
        'DATE': 'datetime',
        'TIMESTAMP': 'datetime',
        'INTERVAL YEAR TO MONTH': 'str',  # Pode ser tratado como uma string
        'INTERVAL DAY TO SECOND': 'str',  # Pode ser tratado como uma string
        'BOOLEAN': 'bool',
        'ROWID': 'str',  # Pode ser tratado como uma string
        'XMLType': 'str',  # Pode ser tratado como uma string
        'URITYPE': 'str',  # Pode ser tratado como uma string
        'ANYDATA': 'object',
        'ANYDATASET': 'object',
        'SDO_GEOMETRY': 'object',
        'SDO_TOPO_GEOMETRY': 'object',
        'SDO_GEORASTER': 'object',
        'OBJECT': 'object',
        'REF CURSOR': 'object',  # Pode ser tratado como um objeto
        'INTEGER': 'int',
        'CHARACTER VARYING': 'object',
        'CHARACTER': 'object',
        'TIMESTAMP WITH TIME ZONE': 'datetime',
        'NUMERIC': 'float',
        'TEXT': 'object'
    }

    # Carrega o DataFrame
    control = pd.read_csv(f'{NOME_PASTA_SCHEMA_CONTROLER}/schema_controler.csv', index_col=0)

    # Convertendo todas as chaves do dicionário para minúsculas
    sql_to_python_types = {key.lower(): value for key, value in sql_to_python_types.items()}

    # Aplicando a conversão para minúsculas tanto na coluna 'data_type' quanto no dicionário
    control['types_python'] = control['data_type'].str.lower().map(sql_to_python_types)

    # Exibe o DataFrame atualizado
    control.to_csv(f'{NOME_PASTA_CONTROL_TABLE}/control_table.csv')

    logging.info(f'Tabela controler criada...\n')

