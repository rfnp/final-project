from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

pd.options.mode.chained_assignment = None  # default='warn'

load_dotenv()

df = pd.read_csv("data/climate-dataset/USW00023169-temperature-degreeF.csv")
# print(df.isna().sum())
df_drop = df.dropna()
# print(df_drop.isna().sum())

df_drop['date'] = pd.to_datetime(df_drop['date'], format='%Y%m%d')

database = os.getenv('PG_DATABASE')
user = os.getenv('PG_USER')
passwd = os.getenv('PG_PASSWORD')
hostname = os.getenv('PG_HOSTNAME')
port = os.getenv('PG_PORT')

conn_string = f'postgresql://{user}:{passwd}@{hostname}:{port}/{database}'
db = create_engine(conn_string)
# conn_engine = db.connect()

try:
    conn = psycopg2.connect(conn_string)
    
    print("Connection success")

except Exception as e:
    print(e)

cur = conn.cursor()

try:
    cur.execute("DROP TABLE IF EXISTS raw_layer.climate_temperature")

    sql_create = """
        CREATE TABLE raw_layer.climate_temperature(
            date date PRIMARY KEY,
            min float,
            max float,
            normal_min float,
            normal_max float
        );
        """

    cur.execute(sql_create)

    conn.commit()
    # conn.close()

    print("Create table success")

except Exception as e:
    print(e)

try:
    sql_insert = f"""
    INSERT INTO raw_layer.climate_temperature(
        date,
        min,
        max,
        normal_min,
        normal_max
    ) VALUES %s 
    """

    # df_drop.to_sql('climate_temperature', schema='raw_layer', con=db, index=False, if_exists='replace', method='multi')
    psycopg2.extras.execute_values(cur, sql_insert, df_drop.values)
    
    conn.commit()
    conn.close()

    cur.close()

    print("Insert to table success")

except Exception as e:
    print(e)