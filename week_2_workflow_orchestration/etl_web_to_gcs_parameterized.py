# https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path
from datetime import timedelta


@task(name='fetch_data_from_url', retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """"read data from url and get in dataframe"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True, name='clean_data')
def clean(df=pd.DataFrame) -> pd.DataFrame:
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(n=2))
    print(f"column:{df.dtypes}")
    print(f"number of rows: {len(df)}")
    return df


@task(name='save_file_locally')
def write_data_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """write datafram local as parquet"""
    path = Path(f"{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(name='save_file_locally')
def write_data_gcs(path: Path) -> None:
    """write datafram local as parquet"""
    gcp_bucket = GcsBucket.load("zoomcamp-bucket")
    gcp_bucket.upload_from_path(path)


@flow(name='etl_web_to_GCS', log_prints=True)
def etl_web_to_gcs(color: str, year: int, month: str) -> None:
    """The manin ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    df = clean(df)
    path = write_data_local(df, dataset_file)
    write_data_gcs(path)
    print (path)


@flow()
def etl_web_to_gcs_parrent(color: str, year: int, month: list[str]):
    """The parrent flow of main flow"""
    for month in month:
        etl_web_to_gcs(color, year, month)


if __name__ == '__main__':
    color = "yellow"
    year = 2021
    month = ["01", "02", "03"]
    etl_web_to_gcs_parrent(color, year, month)

