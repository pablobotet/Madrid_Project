# Databricks notebook source
# DBTITLE 1,BASIC SETUP
import sys
import re

sys.path.append('/Workspace/Users/pablobotet@gmail.com/Madrid_Project/common')
from util import read_xls_from_s3, get_file_keys_from_s3_folder, unzip_and_upload_files_to_s3,read_json_schema
from pyspark.sql.functions import col, lit, monotonically_increasing_id,year, month,when

aws_access_key_id=dbutils.secrets.get("credentials", "AWS_user_id")
aws_secret_access_key=dbutils.secrets.get("credentials", "AWS_secret_access_key")
lista_keys=get_file_keys_from_s3_folder('raw-data-bicimad', 'datos-accidentes', aws_access_key_id, aws_secret_access_key)

for name in lista_keys:
    file_path=f"s3://raw-data-bicimad/{name}"
    print(file_path)
    # Extract year from the filename using regular expression
    year_match = re.search(r'\d{4}', name)
    if year_match:
        year_from_name = year_match.group(0)
    else:
        year_from_name = "Unknown"  # Handle files without a year in their name as needed
    if name.endswith(".csv"):
        df = spark.read.csv(
            path=file_path,
            header=True,
            sep=";",
        )
        df = df.select(
            col("num_expediente").alias("id").cast("string"),
            col("hora").alias("hour").cast("timestamp"),
            col("localizacion").alias("location_street").cast("string"),
            col("numero").alias("location_number").cast("integer"),
            col("cod_distrito").alias("district_id").cast("integer"),
            col("tipo_accidente").alias("accident_type").cast("string"),
            col("estado_meteorológico").alias("weather").cast("string"),
            col("tipo_vehiculo").alias("vehicle").cast("string"),
            col("tipo_persona").alias("person_involved").cast("string"),
            col("rango_edad").alias("age_range").cast("string"),
            when(col("positiva_alcohol")=='S',True).otherwise(False).alias("alcohol").cast("boolean"),
            when(col("positiva_droga")=='NULL',False).otherwise(True).alias("drugs").cast("boolean"),
            lit(year_from_name).alias("year").cast("integer")
        )
        df.write.mode("overwrite").partitionBy("year").parquet("s3://curated-data-bicimad/accidents/")
    
