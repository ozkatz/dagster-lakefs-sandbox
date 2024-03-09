from dataclasses import dataclass
from dagster import Definitions, job, op, Config, OpExecutionContext

import pandas as pd


class RawConfig(Config):
    raw_path_uri: str

class PathConfig(Config):
    base_path: str


@op
def get_yolo_uri(config: PathConfig) -> str:
    return config.base_path


@op
def raw_taxi_data(config: RawConfig, base_uri: str) -> str:
    df = pd.read_parquet(config.raw_path_uri)
    ingest_location = f'{base_uri}/raw/data.parquet'
    df.to_parquet(ingest_location, compression='snappy')
    return ingest_location


@op
def clean_nyc_taxi(base_uri: str, raw_taxi_data: str) -> str:
    df = pd.read_parquet(raw_taxi_data)
    df = df.query('passenger_count >= 0')  # clean up!
    output_path = f'{base_uri}/clean/data.parquet'
    df.to_parquet(output_path)
    return output_path


@op
def partition_nyc_taxi(base_uri: str, clean_taxi_data: str) -> str:
    df = pd.read_parquet(clean_taxi_data)
    df['year'] = df['tpep_pickup_datetime'].dt.year
    df['month'] = df['tpep_pickup_datetime'].dt.month
    df['day'] = df['tpep_pickup_datetime'].dt.day
    output_path = f'{base_uri}/partitioned/'
    df.to_parquet(output_path, partition_cols=['year', 'month', 'day'], existing_data_behavior='delete_matching')
    return output_path

@job
def nyc_taxi_job():
    base_uri = get_yolo_uri()
    ingested_loc = raw_taxi_data(base_uri)
    clean_loc = clean_nyc_taxi(base_uri, ingested_loc)
    partitioned_location = partition_nyc_taxi(base_uri, clean_loc)


defs = Definitions(jobs=[nyc_taxi_job])
