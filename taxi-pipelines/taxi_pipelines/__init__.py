from dagster import Definitions, load_assets_from_modules, define_asset_job

from . import assets

all_assets = load_assets_from_modules([assets])

nyc_taxi_job = define_asset_job(
    name='taxi_asset_job', 
    selection=['nyc_taxi', 'clean_nyc_taxi', 'day_partitioned_nyc_taxi']
)

defs = Definitions(
    assets=all_assets,
    jobs=[nyc_taxi_job],
)
