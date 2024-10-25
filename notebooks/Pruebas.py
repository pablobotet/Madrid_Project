# Databricks notebook source
import sys

sys.path.append('/Workspace/Users/pablobotet@gmail.com/Madrid_Project/common')

from util import read_file
aws_access_key_id=dbutils.secrets.get("credentials", "AWS_user_id")
aws_secret_access_key=dbutils.secrets.get("credentials", "AWS_secret_access_key")

district_df=read_xls_from_s3(bucket_name='prueba-acceso',file_key='DISTT0123.xlsx',AWS_access_key_id=aws_access_key_id,AWS_secret_access_key=aws_secret_access_key)
df=read_file(bucket_name='raw-data-bicimad/datos-bicimad', file_key='trips_22_02_February-csv.zip', AWS_access_key_id=aws_access_key_id, AWS_secret_access_key=aws_secret_access_key)
