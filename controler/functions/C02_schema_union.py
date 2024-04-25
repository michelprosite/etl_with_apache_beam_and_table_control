import os
import pandas as pd
import logging

def union(): 

    logging.info(f'Iniciando separação do tipos das colunas do banco!\n')

    list_shemas_tables = os.listdir('/home/michel/Documentos/Projetos/etl_with_apache_beam_and_table_control/controler/data/schemas_origin')
    for table in list_shemas_tables:
        new = pd.read_csv(f'/home/michel/Documentos/Projetos/etl_with_apache_beam_and_table_control/controler/data/schemas_origin/{table}')
        new = new.iloc[:, -1]  # Selecionando apenas a última coluna
        if os.path.exists('/home/michel/Documentos/Projetos/etl_with_apache_beam_and_table_control/controler/data/schema_controler/schema_controler.csv'):
            old = pd.read_csv('/home/michel/Documentos/Projetos/etl_with_apache_beam_and_table_control/controler/data/schema_controler/schema_controler.csv')
            old = old.iloc[:, -1]  # Selecionando apenas a última coluna
            new_df = pd.concat([old, new]).drop_duplicates()
            new_df.to_csv('/home/michel/Documentos/Projetos/etl_with_apache_beam_and_table_control/controler/data/schema_controler/schema_controler.csv')
            logging.info(f'Schema {table} salvo!\n')
        else:
            new_df = new.drop_duplicates() 
            new_df.to_csv('/home/michel/Documentos/Projetos/etl_with_apache_beam_and_table_control/controler/data/schema_controler/schema_controler.csv')  
            logging.info(f'Schema {table} salvo!\n')



