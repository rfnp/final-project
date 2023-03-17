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
# conn_engine = db.connect()

try:
    conn = psycopg2.connect(conn_string)
    
    print("Connection success")

except Exception as e:
    print(e)

cur = conn.cursor()

# sql_drop_fkey = """
#     ALTER TABLE raw_layer.climate_temperature DROP CONSTRAINT IF EXISTS fk_temperature_precipitation;
#     ALTER TABLE raw_layer.yelp_checkin DROP CONSTRAINT IF EXISTS fk_checkin_business;
#     ALTER TABLE raw_layer.yelp_tip DROP CONSTRAINT IF EXISTS fk_tip_user;
#     ALTER TABLE raw_layer.yelp_tip DROP CONSTRAINT IF EXISTS fk_tip_business;
#     ALTER TABLE raw_layer.yelp_review DROP CONSTRAINT IF EXISTS fk_review_business;
#     ALTER TABLE raw_layer.yelp_review DROP CONSTRAINT IF EXISTS fk_review_precipitation; 
#     """

# cur.execute(sql_drop_fkey)
# conn.commit()
# conn.close()
# cur.close()

try:
    sql_add_fkey_temperature = """
        ALTER TABLE raw_layer.climate_temperature DROP CONSTRAINT IF EXISTS fk_temperature_precipitation;
        ALTER TABLE raw_layer.climate_temperature ADD CONSTRAINT fk_temperature_precipitation FOREIGN KEY (date) REFERENCES raw_layer.climate_precipitation (date);"""

    sql_add_fkey_checkin = """
        ALTER TABLE raw_layer.yelp_checkin DROP CONSTRAINT IF EXISTS fk_checkin_business;
        ALTER TABLE raw_layer.yelp_checkin ADD CONSTRAINT fk_checkin_business FOREIGN KEY (business_id) REFERENCES raw_layer.yelp_business (business_id);"""

    sql_add_fkey_tip = """
        ALTER TABLE raw_layer.yelp_tip DROP CONSTRAINT IF EXISTS fk_tip_user;
        ALTER TABLE raw_layer.yelp_tip DROP CONSTRAINT IF EXISTS fk_tip_business;
        ALTER TABLE raw_layer.yelp_tip ADD CONSTRAINT fk_tip_user FOREIGN KEY (user_id) REFERENCES raw_layer.yelp_user (user_id);
        ALTER TABLE raw_layer.yelp_tip ADD CONSTRAINT fk_tip_business FOREIGN KEY (business_id) REFERENCES raw_layer.yelp_business (business_id);"""

    sql_add_fkey_review = """
        ALTER TABLE raw_layer.yelp_review DROP CONSTRAINT IF EXISTS fk_review_business;
        ALTER TABLE raw_layer.yelp_review DROP CONSTRAINT IF EXISTS fk_review_precipitation;       
        ALTER TABLE raw_layer.yelp_review ADD CONSTRAINT fk_review_business FOREIGN KEY (business_id) REFERENCES raw_layer.yelp_business (business_id);
        ALTER TABLE raw_layer.yelp_review ADD CONSTRAINT fk_review_precipitation FOREIGN KEY (date) REFERENCES raw_layer.climate_precipitation (date);"""

    cur.execute(sql_add_fkey_temperature)
    cur.execute(sql_add_fkey_checkin)
    cur.execute(sql_add_fkey_tip)
    cur.execute(sql_add_fkey_review)

    conn.commit()
    conn.close()

    cur.close()

    print("Make relation success")

except Exception as e:
    print(e)