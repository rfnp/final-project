from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import sqlalchemy

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

load_dotenv()

df = pd.read_json("data/yelp-dataset/yelp_academic_dataset_business.json", lines=True)
# print(df.isna().sum())
# print(df[:35])
# print(df.dtypes)
# print(df['hours'])
# df_drop = df.dropna()
# print(df_drop.isna().sum())

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
    cur.execute("DROP TABLE IF EXISTS raw_layer.yelp_business")

    sql_create = """
        CREATE TABLE raw_layer.yelp_business(
            business_id TEXT PRIMARY KEY,
            name TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            postal_code TEXT,
            latitude float,
            longitude float,
            stars int,
            review_count int,
            is_open boolean,
            attributes json,
            categories TEXT,
            hours json
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
    INSERT INTO raw_layer.yelp_business(
        business_id,
        name,
        address,
        city,
        state,
        postal_code,
        latitude,
        longitude,
        stars,
        review_count,
        is_open,
        attributes,
        categories,
        hours
    ) VALUES %s 
    """

    # psycopg2.extras.execute_values(cur, sql_insert, df.values)
    df.to_sql('yelp_business', schema='raw_layer', con=db, dtype={'attributes': sqlalchemy.types.JSON, 'hours': sqlalchemy.types.JSON}, index=False, if_exists='replace', method='multi')


    sql_add_pkey = "ALTER TABLE raw_layer.yelp_business ADD PRIMARY KEY (business_id)"
    cur.execute(sql_add_pkey)
    
    conn.commit()
    conn.close()

    cur.close()

    print("Insert to table success")

except Exception as e:
    print(e)