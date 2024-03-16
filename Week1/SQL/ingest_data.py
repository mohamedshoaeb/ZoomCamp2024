#!/usr/bin/env python
# coding: utf-8

import os
'''
os.system(f"pip3 install --upgrade pip3")
os.system(f"pip3 install pandas")
os.system(f"pip3 install jupyter_contrib_nbextensions")
os.system(f"pip3 install pyarrow")
os.system(f"pip3 install sqlalchemy")
os.system(f"pip3 install pg8000")
'''

import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse
import datetime




# DEFINE THE DATABASE CREDENTIALS
user = 'root'
password = 'root'
host = 'localhost'
port = 5432
database = 'ny_taxi'


# PYTHON FUNCTION TO CONNECT TO THE MYSQL DATABASE AND
# RETURN THE SQLACHEMY ENGINE OBJECT
def get_connection(user, password, host, port, database):
    return create_engine(
        url="postgresql+pg8000://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        )
    )








def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table_name = params.table_name
    url_download_parquet = params.url_download_parquet



    parquet_name = "Output1.parquet"
    csv_name = 'Output1.csv'

    print(f"will run this command wget {url_download_parquet} -O " +  parquet_name + "")
    os.system(f"wget {url_download_parquet} -O " +  parquet_name + "")


    try:    
        # GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
        engine = get_connection(user, password, host, port, database)
        print(
        f"Connection to the {host} for user {user} created successfully.")
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)


    df = pd.read_parquet( parquet_name , engine='pyarrow' )
    df.to_csv(csv_name)


    df_len = pd.read_csv(csv_name)
    lenth_of_df = len(df_len)
    del df_len
    print(f'we will insert {lenth_of_df} Rows into {table_name} ')

    df_iterator = pd.read_csv(csv_name, iterator = True, chunksize=100000)

    df = next(df_iterator)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df = df.iloc[: , 1:]
    print(f'we will start insert now {time()}')
    df.head(n=0).to_sql(name = table_name, con=engine, if_exists='replace')
    print('Table Created successfully ' )  
    print(pd.io.sql.get_schema(df, 'yellow_Taxi_data_2021_01', con=engine))
    df.to_sql(name = table_name, con=engine, if_exists='append')
    print('First chunk was inserted .........' ) 

    while True:
        t_start = time()
        formated_t_start = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        
        df = next(df_iterator)
        df = df.iloc[: , 1:]
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        
        #insert statment
        df.to_sql(name = table_name, con=engine, if_exists='append')

        t_end = time()
        formated_t_end = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))




        print(
        'Inserted another chunk \n From  %s  To %s ........., \n Took %.3f min \n ---------------------------------------------------------------' 
        % (formated_t_start , formated_t_end , ((t_end - t_start) / 60 )) )   

        if len(df) != 100000:
            break




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest Data from parquet To Postgresql .')
    parser.add_argument('--user', help='User Name For Postgresql')
    parser.add_argument('--password', help='Password Name For Postgresql')
    parser.add_argument('--host', help='Host For Postgresql')
    parser.add_argument('--port', help='Port For Postgresql')
    parser.add_argument('--database', help='database Name For Postgresql')
    parser.add_argument('--table_name', help='Name Of tabel will put resualt in postgres')
    parser.add_argument('--url_download_parquet', help='url for dowanload parquet file')


    args = parser.parse_args()
    main(args)

    


