import sys
import os
from glob import glob
from minio import Minio

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient
from tempfile import NamedTemporaryFile
import requests

###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "./config/datalake.yaml"
# YEARS = ["2020", "2021", "2022", "2023"]
YEARS = ["2024"]
###############################################


###############################################
# Main
###############################################
def extract_load(endpoint_url, access_key, secret_key):
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    nyc_data_cfg = cfg["nyc_data"]

    client = MinIOClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )

    # client.create_bucket(datalake_cfg["bucket_name_1"])
    client.create_bucket("raw")

    # print("extract_load is running... ")
    # dir_path = os.path.dirname(os.path.realpath(__file__))

    # print(f"dir_path: {dir_path}")

    # for year in YEARS:
    #     # Upload files
    #     all_fps = glob(os.path.join(nyc_data_cfg["folder_path"], year, "*.parquet"))

    #     print(os.path.join(nyc_data_cfg["folder_path"], year))
    #     print(f"all_fps: {all_fps}")

    #     for fp in all_fps:
    #         print(f"Uploading {fp}")
    #         client_minio = client.create_conn()
    #         client_minio.fput_object(
    #             bucket_name=datalake_cfg["bucket_name_1"],
    #             object_name=os.path.join(datalake_cfg["folder_name"], os.path.basename(fp)),
    #             file_path=fp,
    #         )
            
# Function to download the file locally
def _download_file(url, local_filepath):
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(local_filepath, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    print(f"Downloaded {local_filepath}")
            
def download_nyc_taxi_data(endpoint_url, access_key, secret_key, data_entity="yellow_tripdata", bucket_name="raw" ,**kwargs):

    execution_date = kwargs['execution_date']
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    
    print(f"Downloading NYC Taxi Data (entity: {data_entity}) | Year: {year} | Month: {month}")

    client = MinIOClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )

    bucket_name = "raw"

    client.create_bucket(bucket_name)
    
    client_minio = client.create_conn()
    
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{data_entity}_{year}-{month}.parquet"
    
    # object_name = url.split("/")[-1]
    object_name = os.path.basename(url)

    # Download the file to a temporary file and upload to MinIO
    with NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_file:
        local_filepath = tmp_file.name
        _download_file(url, local_filepath)
        
        client_minio.fput_object(
            bucket_name=bucket_name,
            object_name=os.path.join(data_entity, year, month , object_name),
            file_path=local_filepath,
        )

    s3_path = f"s3a://{bucket_name}/{data_entity}/{year}/{month}/{object_name}"
    print(f"File {object_name} uploaded to s3 path: {s3_path}")

    # Return the s3_path to push it to XCom
    kwargs['ti'].xcom_push(key='s3_path', value=s3_path)

    # Optionally, return the s3_path
    return s3_path
    
    
###############################################


# if __name__ == "__main__":
#     cfg = load_cfg(CFG_FILE)
#     datalake_cfg = cfg["datalake"]

#     ENDPOINT_URL_LOCAL = datalake_cfg['endpoint']
#     ACCESS_KEY_LOCAL = datalake_cfg['access_key']
#     SECRET_KEY_LOCAL = datalake_cfg['secret_key']

#     download_nyc_taxi_data(ENDPOINT_URL_LOCAL, ACCESS_KEY_LOCAL, SECRET_KEY_LOCAL)