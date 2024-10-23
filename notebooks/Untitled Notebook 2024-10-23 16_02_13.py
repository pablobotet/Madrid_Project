# Databricks notebook source
import sys

sys.path.append('/Workspace/Users/pablobotet@gmail.com/Madrid_Project/common')
from util import read_xls_from_s3
#Ahora tenemos que obtener los datos del xls de los distritos
# Try to access dbutils via the get_ipython method as a workaround
# Assuming dbutils is available in the Databricks environment
# Access AWS credentials stored in Databricks secrets
aws_access_key_id=dbutils.secrets.get("credentials", "AWS_user_id")
aws_secret_access_key=dbutils.secrets.get("credentials", "AWS_secret_access_key")

district_df=read_xls_from_s3(bucket_name='prueba-acceso',file_key='DISTT0123.xlsx',AWS_access_key_id=aws_access_key_id,AWS_secret_access_key=aws_secret_access_key)

district_df.drop(columns=district_df.columns[0], axis=1,  inplace=True)
district_df.rename(columns={'Unnamed: 1': 'Magnitude'}, inplace=True)
district_df[district_df['Magnitude'].str.contains('Características|Superficie|Población a 01/01/2023|31/12/2022|turismos',case=False, na=False)]
district_df=district_df[district_df['Magnitude'].str.contains('Características|Superficie|Población a 01/01/2023|31/12/2022|turismos',case=False, na=False)]
district_df.iloc[0]=['district_id']+[i for i in range(0,22)]
district_df.drop(district_df.index[-1],inplace=True)
district_df=district_df.T

print(district_df)
# Convert the Pandas DataFrame to a Spark DataFrame


# Define the path where you want to save the Delta table
# This can be a path in DBFS or an S3 bucket
delta_table_path = "dbfs:/databricks-datasets/district_delta_table"



# COMMAND ----------


