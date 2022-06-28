#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pandas as pd
import numpy as np

from time import time
from sqlalchemy import create_engine


def main(paramas):
    user = paramas.user
    password = paramas.password
    host = paramas.host
    port = paramas.port
    db = paramas.db
    table_name = paramas.table_name
    url = paramas.url
    file_name = 'output.parquet'

    os.system(f'wget {url} -O {file_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_parquet(file_name)
    print(df.head())

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    no_chuncks = int(len(df) / 50000)
    print("Starting data ingestion...")
    for df_chunk in np.array_split(df, no_chuncks):
        t_start = time()
        
        df_chunk.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df_chunk.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df_chunk.to_sql(name=table_name, con=engine, if_exists='append', method='multi')
        
        t_end = time()
        print(f"Imported chunk..., size: {len(df_chunk)}, took: {t_end - t_start:.2f} s")
    print("Data ingestion finished sucessfully.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we eill write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)