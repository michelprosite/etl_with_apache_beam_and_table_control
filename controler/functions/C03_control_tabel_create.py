import pandas as pd 
import logging

def create_table_control():
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
    control = pd.read_csv('/home/michel/Documentos/Projetos/etl_with_apache_beam_and_table_control/controler/data/schema_controler/schema_controler.csv', index_col=0)

    # Convertendo todas as chaves do dicionário para minúsculas
    sql_to_python_types = {key.lower(): value for key, value in sql_to_python_types.items()}

    # Aplicando a conversão para minúsculas tanto na coluna 'data_type' quanto no dicionário
    control['types_python'] = control['data_type'].str.lower().map(sql_to_python_types)

    # Exibe o DataFrame atualizado
    control.to_csv('/home/michel/Documentos/Projetos/etl_with_apache_beam_and_table_control/controler/data/control_table/control_table.csv')

    logging.info(f'Tabela controler criada...\n')

