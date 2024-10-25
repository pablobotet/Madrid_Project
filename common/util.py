import os
import boto3
import pandas as pd
from io import StringIO, BytesIO
import boto3
import zipfile
from databricks.sdk.runtime import *
import re
from pyspark.sql import DataFrame
import tempfile
import shutil

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

def get_file_keys_from_s3_folder(bucket_name:str, folder_name:str, aws_access_key_id:str, aws_secret_access_key:str):
    # Crear un cliente de S3
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        
        # Listar objetos en la carpeta especificada
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_name + '/'
        )
        
        # Extraer las claves de archivo de la respuesta
        file_keys = [item['Key'] for item in response.get('Contents', [])]
        
        return file_keys
    except Exception as e:
        print("Could not connect to the s3 client")
        
import os
import boto3
import pandas as pd
from io import StringIO, BytesIO
import boto3
import zipfile
from databricks.sdk.runtime import *
import re
from pyspark.sql import DataFrame
import tempfile
import shutil

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

def get_file_keys_from_s3_folder(bucket_name:str, folder_name:str, aws_access_key_id:str, aws_secret_access_key:str):
    # Crear un cliente de S3
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        
        # Listar objetos en la carpeta especificada
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_name + '/'
        )
        
        # Extraer las claves de archivo de la respuesta
        file_keys = [item['Key'] for item in response.get('Contents', [])]
        
        return file_keys
    except Exception as e:
        print("Could not connect to the s3 client")
        
def unzip_and_upload_files_to_s3(bucket_name, zip_file_key, target_s3_folder, AWS_access_key_id, AWS_secret_access_key):
    # Create a session using stored AWS credentials
    session = boto3.Session(
        aws_access_key_id=AWS_access_key_id,
        aws_secret_access_key=AWS_secret_access_key,
    )
    s3_client = session.client('s3')
    
    # Get the ZIP file from S3
    zip_obj = s3_client.get_object(Bucket=bucket_name,Key=zip_file_key)
    zip_content = zip_obj['Body'].read()

    # Create a temporary directory to extract the ZIP file
    with tempfile.TemporaryDirectory() as tmpdirname:
        with BytesIO(zip_content) as tf, zipfile.ZipFile(tf, mode='r') as zipf:
            zipf.extractall(tmpdirname)
            # Iterate through the files in the temporary directory
            for file_name in os.listdir(tmpdirname):
                file_path = os.path.join(tmpdirname, file_name)
                # Define the S3 path where the file will be uploaded
                s3_target_path = f"{target_s3_folder}/{file_name}"
                
                # Upload the file to S3
                s3_client.upload_file(Filename=file_path, 
                                      Bucket=bucket_name, 
                                      Key=s3_target_path)
                print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_target_path}. Ups")
aws_access_key_id=dbutils.secrets.get("credentials", "AWS_user_id")
aws_secret_access_key=dbutils.secrets.get("credentials", "AWS_secret_access_key")

