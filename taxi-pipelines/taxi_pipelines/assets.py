import json
import os

import pandas as pd

from dagster import asset, EnvVar, Definitions, define_asset_job

warehouse_path = EnvVar('WAREHOUSE_PATH')
EXTERNAL_INPUT_SOURCE = 's3://nyc-tlc/trip data/yellow_tripdata_2023-06.parquet'


@asset
def nyc_taxi():
    """
    NYC Taxi data ingested from AWS' Open Datasets
    """
    df = pd.read_parquet(EXTERNAL_INPUT_SOURCE)
    df.to_parquet(f'{warehouse_path.get_value()}/nyc_taxi/raw/data.parquet')


@asset(deps=[nyc_taxi])
def clean_nyc_taxi():
    df = pd.read_parquet(f'{warehouse_path.get_value()}/nyc_taxi/raw/data.parquet')
    df = df.query('passenger_count >= 0')  # clean up!
    df.to_parquet(f'{warehouse_path.get_value()}/nyc_taxi/clean/data.parquet')


@asset(deps=[clean_nyc_taxi])
def day_partitioned_nyc_taxi():
    df = pd.read_parquet(f'{warehouse_path.get_value()}/nyc_taxi/clean/data.parquet')
    df['year'] = df['tpep_pickup_datetime'].dt.year
    df['month'] = df['tpep_pickup_datetime'].dt.month
    df['day'] = df['tpep_pickup_datetime'].dt.day
    df.to_parquet(
        f'{warehouse_path.get_value()}/nyc_taxi/partitioned/data.parquet', 
        partition_cols=['year', 'month', 'day'],
        existing_data_behavior='delete_matching',
    )

