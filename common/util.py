import os
import boto3
import pandas as pd
from io import StringIO, BytesIO
import boto3
import zipfile
from databricks.sdk.runtime import *
import re
from pyspark.sql import DataFrame

def read_xls_from_s3(bucket_name: str, file_key: str, AWS_access_key_id:str, AWS_secret_access_key:str):
    # Crear una sesión utilizando las credenciales de AWS almacenadas
    session = boto3.Session(
        aws_access_key_id=AWS_access_key_id,
        aws_secret_access_key=AWS_secret_access_key,
    )
    s3_client = session.client('s3')
    # Leer el archivo .xls desde S3
    xls_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    body = xls_obj['Body'].read()
    #Creamos el DataFrame
    df = pd.read_excel(body)
    return df

def write_df_to_s3(df:DataFrame,bucket_name:str, partition_by=None)->None:
    destination_path = f"s3a://{bucket_name}/data_partitioned/"
    if partition_by==None:
        try:
            df.write.partitionBy("month").format("parquet").mode("overwrite").save(destination_path)
            
            print(f"Data has been successfully loaded.")
        except Exception as e:
            print("Mistake loading file to bucket")
    else:
        try:
            df.write.partitionBy(f"{partition_by}").format("parquet").mode("overwrite").save(destination_path)

            print(f"Data has been successfully partitioned by {partition_by}.")
        except Exception as e:
            print("Mistake loading file to bucket")
    pass

def read_file(bucket_name: str, file_key: str, AWS_access_key_id:str, AWS_secret_access_key:str):
    # Create a session using stored AWS credentials
    session = boto3.Session(
        aws_access_key_id=AWS_access_key_id,
        aws_secret_access_key=AWS_secret_access_key,
    )
    s3_client = session.client('s3')
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    
    # Check if the file is a ZIP file
    if 'zip' in obj['ContentType']:
        # Download the ZIP file
        zip_content = obj['Body'].read()
        with BytesIO(zip_content) as tf:
            # Open the ZIP file
            with zipfile.ZipFile(tf, mode='r') as zipf:
                # List contents of the ZIP file to determine the format
                for file_name in zipf.namelist():
                    if file_name.endswith('.json'):
                        # Assuming the ZIP contains a JSON file
                        with zipf.open(file_name) as f:
                            return spark.read.json(f)
                    elif file_name.endswith('.csv'):
                        # Assuming the ZIP contains a CSV file
                        with zipf.open(file_name) as f:
                            return spark.read.csv(f, header=True, inferSchema=True)
    elif '.json' in obj['ContentType']:
        # Read as JSON
        return spark.read.json("s3a://" + bucket_name + "/" + file_key)
    elif '.csv' in obj['ContentType']:
        # Read as CSV
        return spark.read.csv("s3a://" + bucket_name + "/" + file_key, header=True, inferSchema=True)
    else:
        raise ValueError("Unsupported file format. Please provide a .json, .csv, or .zip file.")






