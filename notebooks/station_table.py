# Databricks notebook source
#Vamos a crear la tabla de las estaciones. En bicimad no hemos encontrado un listado de las estaciones como tal. Para solventar este problema vamos a usar los datos de la tabla de viajes.import sys
sys.path.append('/Workspace/Users/pablobotet@gmail.com/Madrid_Project/common')
from geoloc import zipcode_to_district,location_to_district
from pyspark.sql.functions import udf, StringType
from util import read_xls_from_s3, get_file_keys_from_s3_folder, unzip_and_upload_files_to_s3,read_json_schema
from pyspark.sql.functions import col, lit, monotonically_increasing_id,year, month, StringType

aws_access_key_id=dbutils.secrets.get("credentials", "AWS_user_id")
aws_secret_access_key=dbutils.secrets.get("credentials", "AWS_secret_access_key")
zipcode_to_district_udf = udf(zipcode_to_district, StringType())
# Leemos el archivo. Necesitamos transformarlo en una vista antes de trabajar con ello en SQL.
"""
import json
def latitud(dict_str):
    if dict_str is not None:
        if 'coordinates' in dict_str:
            return eval(dict_str)['coordinates'][1]
    return None

def longitud(dict_str):
    if dict_str is not None:
        if 'coordinates' in dict_str:
            print(eval(dict_str))
            return eval(dict_str)['coordinates'][0]
    return None
"""
def coordinates_to_district(latitud,longitud):
    if latitud is not None and longitud is not None:
        return location_to_district((latitud,longitud))
    return None
    
"""
latitud_udf = udf(latitud, StringType())
longitud_udf = udf(longitud, StringType())
spark.udf.register("latitud_sql", latitud_udf)
spark.udf.register("longitud_sql", longitud_udf)
"""
coordinates_to_district_udf = udf(coordinates_to_district, StringType())

spark.udf.register("coordinates_to_district_sql", coordinates_to_district_udf)

df = spark.read.parquet("s3://curated-data-bicimad/trips/year=2022/")


df = df.select(
            col("unlock_id"),
            col("travel_time"),
            col("latitud"),
            col("longitud")
        )

#df = df.withColumn("zip_code", location_to_zipcode_mod_udf(col("latitud"),col("longitud")))
display(df)
df.createOrReplaceTempView("temp_view")

new_df = spark.sql("""
    SELECT 
        unlock_id AS id, AVG(travel_time) AS travel_time, COUNT(*) AS number_of_trips,  FIRST(latitud, TRUE),FIRST(longitud, TRUE), coordinates_to_district_sql(FIRST(latitud,TRUE ),FIRST(longitud,TRUE)) AS district_id
    FROM temp_view
    GROUP BY unlock_id
    ORDER BY unlock_id
""")
display(df)

new_df.write.format("delta").mode("overwrite").save("s3://curated-data-bicimad/single-files/station-table/")
