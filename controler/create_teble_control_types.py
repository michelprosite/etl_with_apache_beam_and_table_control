from functions.s01_create_with_postgres import create_table_control_types_postgres
from functions.s02_create_with_oracle import create_table_control_types_oracle
import json
import os
import sys

# Start Info
type_db = 'oracle'

path_txt_type_db = "/home/michel/Documentos/Projetos/etl_with_apache_beam_and_table_control/controler/functions/type_db.txt"

def create_type_db_arquive(type_db):
    with open(path_txt_type_db, 'w') as tdb:
        tdb.write(type_db.lower())

create_type_db_arquive(type_db)

path_type_db = 'config/type_db.json'

with open(path_type_db) as t:
    config = json.load(t)

    os.environ['TYPE_DB_POSTGRES'] = config['TYPE_DB_POSTGRES']
    os.environ['TYPE_DB_ORACLE'] = config['TYPE_DB_ORACLE']

    TYPE_DB_POSTGRES = os.getenv('TYPE_DB_POSTGRES')
    TYPE_DB_ORACLE = os.getenv('TYPE_DB_ORACLE')

if TYPE_DB_POSTGRES == str.lower(type_db):
    create_table_control_types_postgres()

elif TYPE_DB_ORACLE == str.lower(type_db):
    create_table_control_types_oracle()

else:
    print(f'\nNo momento não temos uma arquitetura para o banco | {type_db} |\n')
    