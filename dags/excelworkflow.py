from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os

POSTGRES_CONN_ID = 'postgres_default'

default_args = {
    'owner': 'Willane Jaures',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

chemin_fichier = "/mnt/c/Users/Willane Jaures/OneDrive/Documents/Willane_New York Citi Bikes_Raw Data - NYCitiBikes.csv.xlsx"

with DAG(
    dag_id="traitement_excel_local",
    default_args=default_args,
    description="Traitement de fichier excel locaux",
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    @task()
    def read_excel_file():
        if not os.path.exists(chemin_fichier):
            raise FileNotFoundError(f"Le fichier {chemin_fichier} n'existe pas")

        df = pd.read_excel(chemin_fichier)
        print(f"Fichier lu avec succès. Nombre de lignes : {len(df)}")
        return df.to_dict(orient='records')  # convert to list of dicts for XCom

    @task()
    def traiter_donnee(data_records):
        df = pd.DataFrame(data_records)
        df = df.dropna()
        df = df.drop_duplicates()
        df = df.rename(columns={
            'Start Time': 'start_time',
            'Stop Time': 'stop_time',
            # add other renames here as needed
        })
        return df.to_dict(orient='records')

    @task()
    def load_excel_data(data_records):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS NYCity (
            start_time TIMESTAMP,
            stop_time TIMESTAMP,
            start_station_id INT,
            start_station_name TEXT,
            end_station_id INT,
            end_station_name TEXT,
            bike_id INT,
            user_type TEXT,
            birth_year INT,
            year INT,
            age_group TEXT,
            trip_duration INT,
            month INT,
            season TEXT,
            temperature INT,
            weekday TEXT
        );
        """)

        for row in data_records:
            cursor.execute("""
            INSERT INTO NYCity (
                start_time, stop_time, start_station_id, start_station_name,
                end_station_id, end_station_name, bike_id, user_type,
                birth_year, year, age_group, trip_duration, month,
                season, temperature, weekday
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.get('start_time'), row.get('stop_time'),
                row.get('start_station_id'), row.get('start_station_name'),
                row.get('end_station_id'), row.get('end_station_name'),
                row.get('bike_id'), row.get('user_type'),
                row.get('birth_year'), row.get('year'), row.get('age_group'),
                row.get('trip_duration'), row.get('month'), row.get('season'),
                row.get('temperature'), row.get('weekday'),
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print("Données insérées avec succès dans PostgreSQL.")

    # Task Dependencies
    raw_data = read_excel_file()
    cleaned_data = traiter_donnee(raw_data)
    load_excel_data(cleaned_data)
