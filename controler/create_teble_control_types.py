from functions.s01_create_with_postgres import create_table_control_types_postgres
from functions.s02_create_with_oracle import create_table_control_types_oracle
import json
import os
import sys

# Start Info
type_db = 'odsdsds'

path_txt_type_db = "/home/michel/Documentos/Projetos/etl_with_apache_beam_and_table_control/controler/functions/type_db.txt"

def create_type_db_arquive(type_db):
    with open(path_txt_type_db, 'w') as tdb:
        tdb.write(type_db.lower())


    if 'postgres' == str.lower(type_db):
        create_table_control_types_postgres()

    elif 'oracle' == str.lower(type_db):
        create_table_control_types_oracle()

    else:
        print(f'\nNo momento não temos uma arquitetura para o banco | {type_db} |\n')


create_type_db_arquive(type_db)
