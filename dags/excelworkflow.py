from airflow import DAG
from datetime import datetime, timedelta 
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import pandas as pd 
import os 





# definition des arguments par defauts

postgres_conn_id = 'postgres_default'

default_args = {
    'owner': 'Willane Jaures',
    'depends_on_past': False,
    'start_date': datetime(2025,5,17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)

}


#definition DAG
with DAG(
    dag_id = "traitement_excel_local",
    default_args = default_args,
    description = "Traitement de fichier excel locaux",
    schedule_interval = timedelta(days = 1)
) as dag:

    # definition du chemin vers le fichier excel
    chemin_fichier = "/mnt/c/Users/Willane Jaures/OneDrive/Documents/Willane_New York Citi Bikes_Raw Data - NYCitiBikes.csv.xlsx"

    @task()
    def read_excel_file(**kwargs):
        if not os.path.exists(chemin_fichier):
            raise FileNotFoundError(f"Le fichier {chemin_fichier} n'existe pas")
        
        df = pd.read_excel(chemin_fichier)
        print(f"Fichier  lu avec succes. Nombre de ligne : {len(df)}")

        return df


    @task()
    def traiter_donnee(read_excel_file):
        
        # Exemple de traitement simple
        df = df.dropna()
        df = df.drop_duplicates()
        df = df.rename(columns: 'Start Time': 'start_time', 'Stop Time': 'stop_time')


        return df

    
    @ task()
    def load_excel_data(traiter_donnee)


