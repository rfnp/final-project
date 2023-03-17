from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

df = pd.read_csv("data/climate-dataset/USW00023169-LAS_VEGAS_MCCARRAN_INTL_AP-precipitation-inch.csv")
# print(df.isna().sum())
df_fill = df.fillna(0)

''' 
"T" values in the Precipitation or Snow category above indicate a "trace" value was recorded. 
https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:USW00023169/detail. 
a trace denotes an amount of precipitation, such as rain or snow, that is greater than zero, but is too small to be measured by standard units or methods of measurement. 
trace snowfall is sometimes treated as equivalent to small numerical amounts (such as 0.03 millimetres (0.001 in). 
https://en.wikipedia.org/wiki/Trace_(precipitation).
'''
condition_t = df_fill['precipitation'] == 'T'
change_t = df_fill.loc[condition_t, 'precipitation'] = 0.001

condition_0 = df_fill['precipitation'] == '0.00'
change_0 = df_fill.loc[condition_0, 'precipitation'] = 0

df_fill['date'] = pd.to_datetime(df_fill['date'], format='%Y%m%d')

print(df_fill)
# print(df_fill.loc[df_fill['precipitation'] == 'T'])
# print(df_fill[:30])

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
    cur.execute("DROP TABLE IF EXISTS raw_layer.climate_precipitation")

    sql_create = """
        CREATE TABLE raw_layer.climate_precipitation(
            date date PRIMARY KEY,
            precipitation float,
            precipitation_normal float
        );
        """

    cur.execute(sql_create)

    conn.commit()
    # conn.close()

    print("Create table success")

except Exception as e:
    print(e)

# print(df_fill)

try:
    sql_insert = f"""
    INSERT INTO raw_layer.climate_precipitation(
        date,
        precipitation,
        precipitation_normal
    ) VALUES %s 
    """

    # df_fill.to_sql('climate_precipitation', schema='raw_layer', con=db, index=False, if_exists='replace', method='multi')
    psycopg2.extras.execute_values(cur, sql_insert, df_fill.values)
    
    conn.commit()
    conn.close()

    cur.close()

    print("Insert to table success")

except Exception as e:
    print(e)