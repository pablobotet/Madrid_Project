# Databricks notebook source
import sys
import pandas as pd

sys.path.append('/Workspace/Users/pablobotet@gmail.com/Madrid_Project/common')




from util import read_xls_from_s3
#Ahora tenemos que obtener los datos del xls de los distritos
# Try to access dbutils via the get_ipython method as a workaround
# Assuming dbutils is available in the Databricks environment
# Access AWS credentials stored in Databricks secrets
aws_access_key_id=dbutils.secrets.get("credentials", "AWS_user_id")
aws_secret_access_key=dbutils.secrets.get("credentials", "AWS_secret_access_key")

district_df=read_xls_from_s3(bucket_name='raw-data-bicimad',file_key='DISTT0123.xlsx',AWS_access_key_id=aws_access_key_id,AWS_secret_access_key=aws_secret_access_key)

district_df.drop(columns=district_df.columns[0], axis=1,  inplace=True)
district_df.rename(columns={'Unnamed: 1': 'Magnitude'}, inplace=True)
district_df[district_df['Magnitude'].str.contains('Características|Superficie|Población a 01/01/2023|31/12/2022|turismos',case=False, na=False)]
district_df=district_df[district_df['Magnitude'].str.contains('Características|Superficie|Población a 01/01/2023|31/12/2022|turismos',case=False, na=False)]
district_df.iloc[0]=['district_id']+[i for i in range(0,22)]
district_df.drop(district_df.index[-1],inplace=True)
district_df=district_df.T
district_df.columns = ['id','name','surface','population','a','average_square_meter_price','number_of_cars']
district_df = district_df[1:]


# Convert the Pandas DataFrame to a Spark DataFrame


# Define the path where you want to save the Delta table
# This can be a path in DBFS or an S3 bucket
delta_table_path = "dbfs:/databricks-results/district_table_correct"

# Convert the Pandas DataFrame to a Spark DataFrame
schema_path = "/Workspace/Users/pablobotet@gmail.com/Madrid_Project/schema/district_schema.json"
spark_district_df = spark.createDataFrame(district_df)

# Save the Spark DataFrame as a Delta table in DBFS
spark_district_df.write.format("delta").mode("overwrite").save(delta_table_path)
# Save the Spark DataFrame as a Delta table in DBFS


# COMMAND ----------


