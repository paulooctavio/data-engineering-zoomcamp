import os
import pandas as pd
import numpy as np

from time import time
from sqlalchemy import create_engine


def ingest_callable(user, password, host, port, db, table_name, file_name):
    print(user, password, host, port, db, table_name, file_name)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('Connection with database established sucessfully.')
    
    df = pd.read_parquet(file_name)
    print(df.head())

    print("Starting data ingestion...")
    
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    no_chuncks = int(len(df) / 100000)
    
    for df_chunk in np.array_split(df, no_chuncks):
        t_start = time()
        
        df_chunk.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df_chunk.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df_chunk.to_sql(name=table_name, con=engine, if_exists='append', method='multi')
        
        t_end = time()
        print(f"Imported chunk..., size: {len(df_chunk)}, took: {t_end - t_start:.2f} s")
    print("Data ingestion finished sucessfully.")
