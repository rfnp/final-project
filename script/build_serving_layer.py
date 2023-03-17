from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import sqlalchemy

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

    df_na.to_csv('serving_layer.csv', index=False, encoding='utf-8')

    # print(df_na[:10])

    cur.execute(sql_create)
    psycopg2.extras.execute_values(cur, sql_insert, df_na.values)

    # conn.commit()
    conn.close()
    conn_engine.close()
    cur.close()

    print("Success")

except Exception as e:
    print(e)