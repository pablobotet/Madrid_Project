# Databricks notebook source
import sys

sys.path.append('/Workspace/Users/pablobotet@gmail.com/Madrid_Project/common')
from util import read_xls_from_s3, get_file_keys_from_s3_folder, unzip_and_upload_files_to_s3,read_json_schema

aws_access_key_id=dbutils.secrets.get("credentials", "AWS_user_id")
aws_secret_access_key=dbutils.secrets.get("credentials", "AWS_secret_access_key")
#En primer lugar tenemos que descomprimir los archivos. En primer lugar cogemos todas los archivos de la carpeta de s3 y los descomprimimos.
"""
#Primero obtenemos todos los file_keys
file_key_list=get_file_keys_from_s3_folder('raw-data-bicimad', 'datos-bicimad/zipped', aws_access_key_id, aws_secret_access_key)
file_key_list=[i for i in file_key_list if '.zip' in i]
#Los descomprimimos y los almacenamos en otra carpeta. 
for file_key in file_key_list: 
    print(file_key)
    unzip_and_upload_files_to_s3(bucket_name='raw-data-bicimad', zip_file_key=file_key, target_s3_folder= 'datos-bicimad/unzipped', AWS_access_key_id=aws_access_key_id, AWS_secret_access_key =aws_secret_access_key)
    print(file_key)
#Los procesamos y los almacenamos en el bucket cleansed.
"""

#Leemos y unimos los distintos csv y json.
csv_file_path = "s3://raw-data-bicimad/datos-bicimad/unzipped/trips_22_12_December.csv"
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", dbutils.secrets.get(scope="credentials", key="AWS_user_id"))
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", dbutils.secrets.get(scope="credentials", key="AWS_secret_access_key"))

lista_unzipped=get_file_keys_from_s3_folder('raw-data-bicimad', 'datos-bicimad/unzipped', aws_access_key_id, aws_secret_access_key)

final_df = spark.createDataFrame([], schema=read_json_schema("/Workspace/Users/pablobotet@gmail.com/Madrid_Project/schema","trip.json"))

for name in lista_unzipped:
    file_path=f"s3://raw-data-bicimad/{name}"
    print(file_path)
    if name.endswith(".csv"):
        df = spark.read.csv(
            path=file_path,
            header=True,
            sep=";",
            # Assumes the first row is the header
        )
        df = df.select(
            col("idBike").alias("id").cast("string"),
            col("trip_minutes").alias("time").cast("float"),
            col("station_unlock").alias("unlock_id"),
            col("station_lock").alias("lock_id"),
            col("unlock_date").cast("date"),
            col("unlocktype").cast("string")
        )
        final_df = final_df.union(df)

    elif name.endswith(".json"):
        df = spark.read.json(
            path=file_path,
            # Assumes the first row is the header
        )
        df = df.select(
            col("_id").alias("id").cast("string"),
            col("travel_time").alias("float"),
            col("idunplug_station").alias("unlock_id").cast("string"),
            col("idplug_station").alias("lock_id").cast("string"),
            col("unplug_hourTime").alias("unlock_date").cast("date")
        )
        df=df.withColumn("unlocktype",lit("STATION"))
        final_df = final_df.union(df)

display(final_df)
