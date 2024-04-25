from functions.S01_create_with_postgres import create_table_control_types_postegres
from functions.S02_create_with_oracle import create_table_control_types_oracle
import json
import os

# Start Info
type_db = 'oracle'

def create_type_db_arquive(type_db):
    with open('controler/functions/type_db.txt', 'w') as tdb:
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
    create_table_control_types_postegres()
elif TYPE_DB_ORACLE == str.lower(type_db):
    create_table_control_types_oracle()
else:
    print(f'No momento não temos uma arquitetura para o banco {type_db}')