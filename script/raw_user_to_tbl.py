from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import json

load_dotenv()

# read json and then convert it to dataframe for easier data manipulation
data = [json.loads(line) for line in open('data/yelp-dataset/yelp_academic_dataset_user.json', 'r')]
df = pd.DataFrame(data)

# drop unnecessary columns because my PC can't handle large file while processing it. 
df_drop = df.drop([
        'elite',
        'friends',
        'compliment_hot',
        'compliment_more',
        'compliment_profile',
        'compliment_cute',
        'compliment_list',
        'compliment_note',
        'compliment_plain',
        'compliment_cool',
        'compliment_funny',
        'compliment_writer',
        'compliment_photos'], axis=1)
# print(df)
# print(data)

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
    cur.execute("DROP TABLE IF EXISTS raw_layer.yelp_user")

    sql_create = """
        CREATE TABLE raw_layer.yelp_user(
            user_id TEXT PRIMARY KEY,
            name TEXT,
            review_count int,
            yelping_since date,
            useful int,
            funny int,
            cool int,
            fans int,
            average_stars float
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
    INSERT INTO raw_layer.yelp_user(
        user_id,
        name,
        review_count,
        yelping_since,
        useful,
        funny,
        cool,
        fans,
        average_stars
    ) VALUES %s
    """

    # using execute values because it is faster and not eating as much resources. to sql is better but it is slower without multi method and eat so much resource
    # df.to_sql('yelp_user', schema='raw_layer', con=db, index=False, if_exists='replace', method='multi')
    psycopg2.extras.execute_values(cur, sql_insert, df_drop.values)
    
    conn.commit()
    conn.close()

    cur.close()

    print("Insert to table success")

except Exception as e:
    print(e)