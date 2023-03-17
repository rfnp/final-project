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
    sql_create_dim_user = """DROP TABLE IF EXISTS dwh_layer.user_dimension;
        CREATE TABLE dwh_layer.user_dimension AS
        SELECT user_id, name, yelping_since
        FROM raw_layer.yelp_user;

        ALTER TABLE dwh_layer.user_dimension ADD PRIMARY KEY (user_id);"""

    sql_create_dim_business = """DROP TABLE IF EXISTS dwh_layer.business_dimension;
        CREATE TABLE dwh_layer.business_dimension AS
        SELECT business_id, name AS business_name, address, city, state, postal_code
        FROM raw_layer.yelp_business;

        ALTER TABLE dwh_layer.business_dimension ADD PRIMARY KEY (business_id);"""

    sql_create_dim_review = """DROP TABLE IF EXISTS dwh_layer.review_dimension;
        CREATE TABLE dwh_layer.review_dimension AS
        SELECT review_id, date, stars
        FROM raw_layer.yelp_review;

        ALTER TABLE dwh_layer.review_dimension ADD PRIMARY KEY (review_id);"""

    sql_create_dim_climate = """DROP TABLE IF EXISTS dwh_layer.climate_dimension;
        CREATE TABLE dwh_layer.climate_dimension AS
        (SELECT climate_precipitation.date, climate_precipitation.precipitation, climate_precipitation.precipitation_normal, climate_temperature.min, climate_temperature.max, climate_temperature.normal_min, climate_temperature.normal_max
        FROM raw_layer.climate_precipitation, raw_layer.climate_temperature
        WHERE climate_temperature.date = climate_precipitation.date);

        ALTER TABLE dwh_layer.climate_dimension ADD PRIMARY KEY (date);"""

    # sql_create_fact_stars = """DROP TABLE IF EXISTS dwh_layer.stars_fact;
    #     CREATE TABLE dwh_layer.stars_fact AS
    #     SELECT user_dimension.user_id, business_dimension.business_id, review_dimension.review_id, climate_dimension.date, review_dimension.stars
    #     FROM dwh_layer.user_dimension, dwh_layer.business_dimension, dwh_layer.review_dimension, dwh_layer.climate_dimension;

    #     ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_user;
    #     ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_business;
    #     ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_review;
    #     ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_climate;

    #     ALTER TABLE dwh_layer.stars_fact ADD CONSTRAINT fk_stars_user FOREIGN KEY (user_id) REFERENCES dwh_layer.user_dimension (user_id);
    #     ALTER TABLE dwh_layer.stars_fact ADD CONSTRAINT fk_stars_business FOREIGN KEY (business_id) REFERENCES dwh_layer.business_dimension (business_id);
    #     ALTER TABLE dwh_layer.stars_fact ADD CONSTRAINT fk_stars_review FOREIGN KEY (review_id) REFERENCES dwh_layer.review_dimension (review_id);
    #     ALTER TABLE dwh_layer.stars_fact ADD CONSTRAINT fk_stars_climate FOREIGN KEY (date) REFERENCES dwh_layer.climate_dimension (date);

    #     ALTER TABLE dwh_layer.stars_fact ADD PRIMARY KEY (user_id,review_id);"""

    # sql_drop_fkey = """ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_user;
    # ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_business;
    # ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_review;
    # ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_climate;"""

    sql_create_fact_stars_1_by_1 = """
    DROP TABLE IF EXISTS dwh_layer.stars_fact;
        CREATE TABLE dwh_layer.stars_fact(
            user_id TEXT,
            business_id TEXT,
            review_id TEXT,
            date date,
            stars float
        );"""

    sql_insert = """
    INSERT INTO dwh_layer.stars_fact(
        user_id,
        business_id,
        review_id,
        date,
        stars
    ) VALUES %s """

    sql_add_fkey = """ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_user;
    ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_business;
    ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_review;
    ALTER TABLE dwh_layer.stars_fact DROP CONSTRAINT IF EXISTS fk_stars_climate;
    
    ALTER TABLE dwh_layer.stars_fact ADD CONSTRAINT fk_stars_user FOREIGN KEY (user_id) REFERENCES dwh_layer.user_dimension (user_id);
    ALTER TABLE dwh_layer.stars_fact ADD CONSTRAINT fk_stars_business FOREIGN KEY (business_id) REFERENCES dwh_layer.business_dimension (business_id);
    ALTER TABLE dwh_layer.stars_fact ADD CONSTRAINT fk_stars_review FOREIGN KEY (review_id) REFERENCES dwh_layer.review_dimension (review_id);
    ALTER TABLE dwh_layer.stars_fact ADD CONSTRAINT fk_stars_climate FOREIGN KEY (date) REFERENCES dwh_layer.climate_dimension (date);"""


    # q1 = pd.read_sql_query('SELECT user_id FROM dwh_layer.user_dimension', con=conn_engine)
    # q2 = pd.read_sql_query('SELECT business_id FROM dwh_layer.business_dimension', con=conn_engine)
    # q3 = pd.read_sql_query('SELECT review_id FROM dwh_layer.review_dimension', con=conn_engine)
    # q4 = pd.read_sql_query("SELECT date FROM dwh_layer.climate_dimension WHERE date >= '2005-02-16'", con=conn_engine)
    # q5 = pd.read_sql_query('SELECT stars FROM dwh_layer.review_dimension', con=conn_engine)

    # df1 = pd.DataFrame(q1)
    # df2 = pd.DataFrame(q2)
    # df3 = pd.DataFrame(q3)
    # df4 = pd.DataFrame(q4)
    # df5 = pd.DataFrame(q5)

    # df = pd.concat([df1,df2,df3,df4,df5], axis=1)
    # df_na = df.where(df.notna(), None)

    # cur.execute(sql_create_dim_user)
    # cur.execute(sql_create_dim_business)
    # cur.execute(sql_create_dim_review)
    # cur.execute(sql_create_dim_climate)

    # # cur.execute(sql_create_fact_stars_1_by_1)
    # psycopg2.extras.execute_values(cur, sql_insert, df_na.values)

    cur.execute(sql_add_fkey)

    # df.to_sql('stars_fact', schema='dwh_layer', con=db, index=False, if_exists='replace', method='multi')

    conn.commit()
    conn.close()
    conn_engine.close()
    cur.close()

    print("DWH success")

except Exception as e:
    print(e)