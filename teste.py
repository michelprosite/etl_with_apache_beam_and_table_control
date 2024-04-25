with open('controler/functions/type_db.txt', 'r') as tdb:
    type_db = tdb.read().strip()  # Lê o conteúdo do arquivo e remove espaços em branco extras

print(type_db)