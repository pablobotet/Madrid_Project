import os
import boto3
import pandas as pd
from io import StringIO

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
 
def get_dbutils(spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils

dbutils = get_dbutils(spark)
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

