from controler.functions.C01_get_schema_origin import read_schema
from controler.functions.C02_schema_union import union
from controler.functions.C03_control_tabel_create import create_table_control
import json

def create_table_control_types():
    read_schema()
    union()
    create_table_control()
create_table_control_types()