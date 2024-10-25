# Databricks notebook source
import sys

sys.path.append('/Workspace/Users/pablobotet@gmail.com/Madrid_Project/common')
from databricks.sdk.runtime import *
from util import read_file
aws_access_key_id=dbutils.secrets.get("credentials", "AWS_user_id")
aws_secret_access_key=dbutils.secrets.get("credentials", "AWS_secret_access_key")

df=read_file(bucket_name='raw-data-bicimad', file_key='datos-bicimad/trips_22_03_March-csv.zip', AWS_access_key_id=aws_access_key_id, AWS_secret_access_key=aws_secret_access_key)
