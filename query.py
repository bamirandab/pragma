from db import AzureSQLClient
from pipeline import CSVPipeline
import re
import os

connection_params = {
    "server": os.getenv('HOST'),
    "database": os.getenv('DB'),
    "user": os.getenv('USER'),
    "password": os.getenv('PASSWORD')
}

client = AzureSQLClient(connection_params)
pipeline = CSVPipeline(db_client=client)


# Ruta donde están los archivos
ruta_archivos = "data/"

# Expresión regular para capturar año y orden del nombre del archivo
patron = re.compile(r"(\d{4})-(\d+)\.csv")

# Lista para almacenar los archivos con su metadata
archivos = []

# Buscar archivos que cumplan con el patrón
for archivo in os.listdir(ruta_archivos):
    match = patron.match(archivo)
    if match:
        año = int(match.group(1))
        orden = int(match.group(2))
        archivos.append((año, orden, archivo))


for archivo in archivos:
    print(f"Procesando archivo: {archivo[2]} (Año: {archivo[0]}, Orden: {archivo[1]})")
    pipeline.load_csv_file("data/" + archivo[2])

total_count, sum_price, min_price, max_price, avg_price = client.fetchall("SELECT count(*), sum(price), min(price), max(price), avg(price) FROM pragma.transactions")[0]
print(f"Consulta de registros desde base de datos → count: {total_count}, sum: {sum_price}, min: {min_price}, max: {max_price}, avg: {avg_price:.2f}")

resumen = pipeline.load_csv_file("data/validation.csv")

total_count, sum_price, min_price, max_price, avg_price = client.fetchall("SELECT count(*), sum(price), min(price), max(price), avg(price) FROM pragma.transactions")[0]
print(f"Consulta de registros desde base de datos → count: {total_count}, sum: {sum_price}, min: {min_price}, max: {max_price}, avg: {avg_price:.2f}")

