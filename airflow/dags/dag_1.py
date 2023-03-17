from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import sqlalchemy

import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

default_args = {
    'owner': 'rfnp',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def raw_checkin():
    load_dotenv()

    df = pd.read_json("data/yelp-dataset/yelp_academic_dataset_checkin.json", lines=True)

    database = os.getenv('PG_DATABASE')
    user = os.getenv('PG_USER')
    passwd = os.getenv('PG_PASSWORD')
    hostname = os.getenv('PG_HOSTNAME')
    port = os.getenv('PG_PORT')

    conn_string = f'postgresql://{user}:{passwd}@{hostname}:{port}/{database}'
    db = sqlalchemy.create_engine(conn_string)

    try:
        conn = psycopg2.connect(conn_string)
        
        print("Connection success")

    except Exception as e:
        print(e)

    cur = conn.cursor()

    try:
        cur.execute("DROP TABLE IF EXISTS raw_layer.yelp_checkin")

        sql_create = """
            CREATE TABLE raw_layer.yelp_checkin(
                business_id TEXT,
                date_checkin TEXT
            );
            """

        cur.execute(sql_create)

        conn.commit()

        print("Create table success")

    except Exception as e:
        print(e)

    try:
        sql_insert = f"""
        INSERT INTO raw_layer.yelp_checkin(
            business_id,
            date_checkin
        ) VALUES %s 
        """

        psycopg2.extras.execute_values(cur, sql_insert, df.values)
        
        conn.commit()
        conn.close()

        cur.close()

        print("Insert to table success")

    except Exception as e:
        print(e)

def make_star_schema_fkey():
    load_dotenv()

    database = os.getenv('PG_DATABASE')
    user = os.getenv('PG_USER')
    passwd = os.getenv('PG_PASSWORD')
    hostname = os.getenv('PG_HOSTNAME')
    port = os.getenv('PG_PORT')

    conn_string = f'postgresql://{user}:{passwd}@{hostname}:{port}/{database}'
    db = sqlalchemy.create_engine(conn_string)
    conn_engine = db.connect()

    try:
        conn = psycopg2.connect(conn_string)
        
        print("Connection success")

    except Exception as e:
        print(e)

    cur = conn.cursor()

    try:

        sql_add_fkey = """ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_user;
        ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_business;
        ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_review;
        ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_climate;
        
        ALTER TABLE dwh_layer.stars_fact ADD CONSTRAINT fk_stars_user FOREIGN KEY (user_id) REFERENCES dwh_layer.user_dimension (user_id);
        ALTER TABLE dwh_layer.stars_fact ADD CONSTRAINT fk_stars_business FOREIGN KEY (business_id) REFERENCES dwh_layer.business_dimension (business_id);
        ALTER TABLE dwh_layer.stars_fact ADD CONSTRAINT fk_stars_review FOREIGN KEY (review_id) REFERENCES dwh_layer.review_dimension (review_id);
        ALTER TABLE dwh_layer.stars_fact ADD CONSTRAINT fk_stars_climate FOREIGN KEY (date) REFERENCES dwh_layer.climate_dimension (date);"""

        cur.execute(sql_add_fkey)

        conn.commit()
        conn.close()
        conn_engine.close()
        cur.close()

        print("DWH success")

    except Exception as e:
        print(e)

def dwh_layer_to_serving_layer():
    load_dotenv()

    database = os.getenv('PG_DATABASE')
    user = os.getenv('PG_USER')
    passwd = os.getenv('PG_PASSWORD')
    hostname = os.getenv('PG_HOSTNAME')
    port = os.getenv('PG_PORT')

    conn_string = f'postgresql://{user}:{passwd}@{hostname}:{port}/{database}'
    db = sqlalchemy.create_engine(conn_string)
    conn_engine = db.connect()

    try:
        conn = psycopg2.connect(conn_string)
        
        print("Connection success")

    except Exception as e:
        print(e)

    cur = conn.cursor()

    try:
        sql_create = """DROP TABLE IF EXISTS serving_layer.rating_and_climate;
            CREATE TABLE serving_layer.rating_and_climate(
                date date,
                stars float,
                precipitation float,
                precipitation_normal float,
                min float,
                max float,
                normal_min float,
                normal_max float
            );"""

        sql_insert = """
        INSERT INTO serving_layer.rating_and_climate(
            date,
            stars,
            precipitation,
            precipitation_normal,
            min,
            max,
            normal_min,
            normal_max
        ) VALUES %s """

        q1 = pd.read_sql_query("SELECT date FROM dwh_layer.climate_dimension WHERE date >= '2005-02-16';", con=conn_engine)
        q2 = pd.read_sql_query("SELECT stars FROM dwh_layer.stars_fact;", con=conn_engine)
        q3 = pd.read_sql_query("SELECT precipitation FROM dwh_layer.climate_dimension WHERE date >= '2005-02-16';", con=conn_engine)
        q4 = pd.read_sql_query("SELECT precipitation_normal FROM dwh_layer.climate_dimension WHERE date >= '2005-02-16';", con=conn_engine)
        q5 = pd.read_sql_query("SELECT min FROM dwh_layer.climate_dimension WHERE date >= '2005-02-16';", con=conn_engine)
        q6 = pd.read_sql_query("SELECT max FROM dwh_layer.climate_dimension WHERE date >= '2005-02-16';", con=conn_engine)
        q7 = pd.read_sql_query("SELECT normal_min FROM dwh_layer.climate_dimension WHERE date >= '2005-02-16';", con=conn_engine)
        q8 = pd.read_sql_query("SELECT normal_max FROM dwh_layer.climate_dimension WHERE date >= '2005-02-16';", con=conn_engine)

        df1 = pd.DataFrame(q1)
        df2 = pd.DataFrame(q2)
        df3 = pd.DataFrame(q3)
        df4 = pd.DataFrame(q4)
        df5 = pd.DataFrame(q5)
        df6 = pd.DataFrame(q6)
        df7 = pd.DataFrame(q7)
        df8 = pd.DataFrame(q8)

        df = pd.concat([df1,df2,df3,df4,df5,df6,df7,df8], axis=1)
        df_na = df.where(df.notna(), None)

        df_na.to_csv('serving_layer.csv', index=False)

        # print(df_na[:10])

        cur.execute(sql_create)
        psycopg2.extras.execute_values(cur, sql_insert, df_na.values)

        conn.commit()
        conn.close()
        conn_engine.close()
        cur.close()

        print("Success")

    except Exception as e:
        print(e)
    
dag_python = DAG(
    dag_id='simple_etl',
    default_args=default_args,
    description='DWH to Serving Layer and then output the data as a csv',
    start_date=days_ago(1),
    schedule_interval='@daily'
)

extract = PythonOperator(
    task_id='extract to raw',
    python_callable=raw_checkin,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag_python,
)

transform = PythonOperator(
    task_id='extract to raw',
    python_callable=make_star_schema_fkey,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag_python,
)

load = PythonOperator(
    task_id='extract to raw',
    python_callable=dwh_layer_to_serving_layer,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag_python,
)

extract >> transform >> load