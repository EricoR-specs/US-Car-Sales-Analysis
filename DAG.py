
=================================================

Milestone 3

Nama  : Muhammad Erico Ricardo
Batch : FTDS-007-BSD

Code ini dibuat untuk melakukan airflow yang bertujuan mengambil data di psotgre,
extract, melakukan pembersihan data, dan terakhir dimasukan ke kibana.

=================================================


import airflow
import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from elasticsearch import Elasticsearch

import psycopg2 as db
import pandas as pd

def queryPostgresql():
    """
    Mengambil data dari PostgreSQL dan menyimpannya dalam bentuk DataFrame.
    Data kemudian disimpan dalam file CSV.
    """
    conn = db.connect(
        host="postgres",
        database="milestone3",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM table_m3")
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=[
        "Index",
        "Year",
        "Make",
        "Model",
        "Trim",
        "Body",
        "Transmission",
        "Vin",
        "State",
        "Condition",
        "Odometer",
        "Color",
        "Interior",
        "Seller",
        "MMR",
        "Selling Price",
        "Sale Date"
    ])  
    print(df)
    df.to_csv('/opt/airflow/dags/Data_Raw.csv',index=False)
    print("-------Data Saved------")


default_args = {
    'owner': 'Erico',
    'start_date': dt.datetime(2024, 9, 11),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=0.1),
    'schedule_interval': '30 13 * * *',
}


def data_cleaning():
    """
    Melakukan pembersihan data yang didapatkan dari PostgreSQL.
    Menghapus data yang duplikat dan data yang hilang.
    Mengubah nama kolom menjadi huruf kecil dan mengganti spasi dengan garis bawah.
    Mengubah nilai kolom 'make' menjadi huruf kecil.
    Data kemudian disimpan dalam file CSV.
    """
    data = pd.read_csv('/opt/airflow/dags/Data_Raw.csv')
    data = data.drop_duplicates()
    data = data.dropna()

    # Lowercase column names and replace whitespace with underscores
    for column in data.columns:
        data.rename(columns={column: column.lower().replace(' ', '_')}, inplace=True)

    # Lowercase the 'make' column
    data['make'] = data['make'].str.lower()

    print(data.head())  # Print the first few rows of the cleaned data
    data.to_csv('/opt/airflow/dags/data_clean.csv',index=False)
    print("-------Data Cleaned and Saved------")


def insertElasticsearch():
    """
    Memasukkan data yang sudah dibersihkan ke Elasticsearch.
    Data di-index sebagai dokumen dengan tipe 'doc' di dalam indek 'table_m3'.
    """
    es = Elasticsearch('http://elasticsearch:9200')
    df = pd.read_csv('/opt/airflow/dags/data_clean.csv')
    for i, r in df.iterrows():
        doc = r.to_json()
        if not es.exists(index="table_m3", doc_type="doc", id=r['vin']):
            res = es.index(index="table_m3", doc_type="doc", body=doc, id=r['vin'])
            print(f"Document with VIN {r['vin']} inserted.")
        else:
            print(f"Document with VIN {r['vin']} already exists and not inserted.")


with DAG('Milestone3', default_args=default_args) as dag:
    getData = PythonOperator(
        task_id='QueryPostgreSQL',
        python_callable=queryPostgresql
    )
    cleanData = PythonOperator(
        task_id='CleanData',
        python_callable=data_cleaning
    )
    insertData = PythonOperator(task_id='InsertDataElasticsearch',
                                python_callable=insertElasticsearch)

    

getData >> cleanData >> insertData