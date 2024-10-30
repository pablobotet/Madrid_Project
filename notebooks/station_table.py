# Databricks notebook source
#Vamos a crear la tabla de las estaciones. En bicimad no hemos encontrado un listado de las estaciones como tal. Para solventar este problema vamos a usar los datos de la tabla de viajes.
import sys
sys.path.append('/Workspace/Users/pablobotet@gmail.com/Madrid_Project/common')
from geoloc import zipcode_to_district
from pyspark.sql.functions import udf, StringType

zipcode_to_district_udf = udf(zipcode_to_district, StringType())
# Leemos el archivo. Necesitamos transformarlo en una vista antes de trabajar con ello en SQL.

df = spark.read.parquet("s3://curated-data-bicimad/trips/")
df.createOrReplaceTempView("temp_view")
display(df)
new_df = spark.sql("""
SELECT unlock_id AS id, AVG(travel_time) AS travel_time, COUNT(*) AS number_of_trips, zip_code,  zipcode_to_district_sql(zip_code) AS district_id
FROM temp_view
GROUP BY unlock_id
ORDER BY unlock_id
""")
display(new_df)
