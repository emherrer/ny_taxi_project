import pandas as pd
from sqlalchemy import create_engine
from time import time
import urllib.request as request
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def download_file(url, csv_name_gz):
    try:
        # Función para mostrar la barra de progreso
        def progress_bar(block_num, block_size, total_size):
                    downloaded = block_num * block_size
                    percent = (downloaded / total_size) * 100
                    print(f"\rDescargando: {percent:.2f}%", end='')

        # Obtener el tamaño total del archivo
        with request.urlopen(url) as response:
            total_size = int(response.headers['Content-Length'])
            
        # Descargar el archivo con barra de progreso
        request.urlretrieve(url, csv_name_gz, reporthook=progress_bar)
        
        print(f"\nArchivo {csv_name_gz} descargado con éxito.")
        return True
    
    except Exception as e:
        print(f"Error descargando el archivo: {e}")
        return False

def process_and_load_to_db(csv_name_gz, user, password, host, port, db, table_name, batch_size):
        
    # Conexión a PostgreSQL
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
       
    # Leer archivo por lotes
    chunk_iter = pd.read_csv(csv_name_gz, compression="gzip", iterator=True, chunksize=batch_size)
    
    # Crear tabla
    next(chunk_iter).head(n=0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    # Insertar datos por lotes
    for i, chunk in enumerate(chunk_iter):
        print(f"Procesando lote {i + 1}...")
        t_start = time()
        
        # Convertir columnas a minusculas y datetime
        chunk["tpep_pickup_datetime"] = pd.to_datetime(chunk["tpep_pickup_datetime"], errors="coerce")
        chunk["tpep_dropoff_datetime"] = pd.to_datetime(chunk["tpep_dropoff_datetime"], errors="coerce")
        
        # Insertar el lote en la base de datos
        chunk.to_sql(table_name, engine, if_exists="append", index=False, method="multi")
        
        t_end = time()
        print(f"Lote {i + 1} insertado con éxito en {round((t_end - t_start), 3)} segundos.")

    print("Carga completada.")


# Definiendo el DAG
# NOTA: Testing data ingestion desde enero a marzo 2021
my_dag = DAG("yellow_taxi_ingestion", 
    schedule_interval="0 6 2 * *", # Se ejecuta el día 2 de cada mes a las 06:00 AM.
    start_date=datetime(2021, 1, 1), 
    end_date=datetime(2021, 3, 28), 
    catchup=True,  #Ejecuta trabajos pasados si no se hicieron antes
    max_active_runs=1
    )

table_name_template = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'    # yellow_taxi_AAAA_MM
csv_name_gz_template = 'output_{{ execution_date.strftime(\'%Y_%m\') }}.csv.gz' # output_AAAA_MM.csv.gz
url_template = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz"

user = "root"
password = "root"
host = "destpg"  #"localhost"
port = "5432"        #"5433"
db = "dest_db"
batch_size= 10000

# Tarea 1
download_task = PythonOperator(
    task_id = "download_file",
    python_callable = download_file,
    op_kwargs = {
        "url": url_template, 
        "csv_name_gz": csv_name_gz_template
        },
    dag=my_dag
)

# Tarea 2
process_task = PythonOperator(
    task_id = "process_and_load_to_db",
    python_callable = process_and_load_to_db,
    op_kwargs = {
        "csv_name_gz": csv_name_gz_template,
         "user": user, 
         "password": password,
         "host": host,
         "port": port, 
         "db": db, 
         "table_name": table_name_template, 
         "batch_size": batch_size
        },
    dag=my_dag
)

#Secuencia
download_task >> process_task