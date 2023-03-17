from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

pd.options.mode.chained_assignment = None  # default='warn'

load_dotenv()

df = pd.read_json("data/yelp-dataset/yelp_academic_dataset_tip.json", lines=True)
print(df.isna().sum())
# df_drop = df.dropna()
# print(df_drop.isna().sum())

# df_drop['date'] = pd.to_datetime(df_drop['date'], format='%Y%m%d')

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
    cur.execute("DROP TABLE IF EXISTS raw_layer.yelp_tip")

    sql_create = """
        CREATE TABLE raw_layer.yelp_tip(
            user_id TEXT,
            business_id TEXT,
            text_tip TEXT,
            date date,
            compliment_count int
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
    INSERT INTO raw_layer.yelp_tip(
        user_id,
        business_id,
        text_tip,
        date,
        compliment_count
    ) VALUES %s 
    """

    # df.to_sql('yelp_tip', schema='raw_layer', con=db, index=False, if_exists='replace', method='multi')
    psycopg2.extras.execute_values(cur, sql_insert, df.values)
    
    conn.commit()
    conn.close()

    cur.close()

    print("Insert to table success")

except Exception as e:
    print(e)