from dataclasses import dataclass
from dagster import Definitions, job, op, Config, OpExecutionContext

import lakefs
import pandas as pd

DAGSTER_RUNS_URL = 'http://127.0.0.1:3000/runs'

class RawConfig(Config):
    raw_path_uri: str

class PathConfig(Config):
    base_path: str

class BranchConfig(Config):
    repository: str
    branch: str


@dataclass
class Sandbox:
    repository: str
    branch: str

@op
def create_sandbox(config: BranchConfig) -> Sandbox:
    lakefs.repository(config.repository).\
        branch(config.branch).\
        create(source_reference='main', exist_ok=True)
    return Sandbox(
        repository=config.repository, 
        branch=config.branch, 
    )


@op
def sandbox_uri(config: PathConfig, sandbox: Sandbox) -> str:
    return f'lakefs://{sandbox.repository}/{sandbox.branch}/{config.base_path}'


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


@op
def commit_changes(context: OpExecutionContext, sandbox: Sandbox, partitioned_location: str):
    branch = lakefs.repository(sandbox.repository).branch(sandbox.branch)
    branch.commit(
        'NYC Taxi Dagster run completed',
        metadata={
            'dagster_run_id': context.run.run_id,
            '::lakefs::Dagster::url[url:ui]': f'{DAGSTER_RUNS_URL}/{context.run.run_id}'
        },
    )


@job
def nyc_taxi_job():
    sandbox = create_sandbox()
    base_uri = sandbox_uri(sandbox)
    ingested_loc = raw_taxi_data(base_uri)
    clean_loc = clean_nyc_taxi(base_uri, ingested_loc)
    partitioned_location = partition_nyc_taxi(base_uri, clean_loc)
    commit_changes(sandbox, partitioned_location)


defs = Definitions(jobs=[nyc_taxi_job])
