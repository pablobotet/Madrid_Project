# Databricks notebook source
# MAGIC %md
# MAGIC BICIMAD_TABLE: En este notebook vamos a procesar las tablas de los viajes de los años 2021 y 2022. En primer lugar usaremos los datos en raw-bicimad, que deberán ser descomprimidos. Tras ello los procesaremos y haremos que adopten la estructura de schema definida para trips.json. 
# MAGIC

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/pablobotet@gmail.com/Madrid_Project/common')
from util import read_xls_from_s3, get_file_keys_from_s3_folder, unzip_and_upload_files_to_s3,read_json_schema
from pyspark.sql.functions import col, lit, monotonically_increasing_id,year, month, StringType
from geoloc import location_to_zipcode
aws_access_key_id=dbutils.secrets.get("credentials", "AWS_user_id")
aws_secret_access_key=dbutils.secrets.get("credentials", "AWS_secret_access_key")

# COMMAND ----------

# DBTITLE 1,DESCOMPRESIÓN


#En primer lugar tenemos que descomprimir los archivos. En primer lugar cogemos todas los archivos de la carpeta de s3 y los descomprimimos.

#Primero obtenemos todos los file_keys
file_key_list=get_file_keys_from_s3_folder('raw-data-bicimad', 'datos-bicimad/zipped', aws_access_key_id, aws_secret_access_key)
file_key_list=[i for i in file_key_list if '.zip' in i]
#Los descomprimimos y los almacenamos en otra carpeta. 
for file_key in file_key_list: 
    print(file_key)
    unzip_and_upload_files_to_s3(bucket_name='raw-data-bicimad', zip_file_key=file_key, target_s3_folder= 'datos-bicimad/unzipped', AWS_access_key_id=aws_access_key_id, AWS_secret_access_key =aws_secret_access_key)
    print(file_key)





# COMMAND ----------

# DBTITLE 1,LECTURA Y PROCESAMIENTO DE LOS ARCHIVOS
#Leemos y unimos los distintos csv y json.

#INICIAMOS SESIÓN PARA PODER ALMACENAR EN S3
csv_file_path = "s3://raw-data-bicimad/datos-bicimad/unzipped/trips_22_12_December.csv"
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", dbutils.secrets.get(scope="credentials", key="AWS_user_id"))
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", dbutils.secrets.get(scope="credentials", key="AWS_secret_access_key"))

#Modificación de la función location
def invertir_lista(lista):
    lista[0],lista[1]=lista[1],lista[0]
    return lista



lista_unzipped=get_file_keys_from_s3_folder('raw-data-bicimad', 'datos-bicimad/unzipped', aws_access_key_id, aws_secret_access_key)

#Leemos los archivos con el schema definido y los almacenamos en el bucket de cleansed.
max_id=0
for name in lista_unzipped:
    file_path=f"s3://raw-data-bicimad/{name}"
    print(file_path)
    if name.endswith(".csv"):
        df = spark.read.csv(
            path=file_path,
            header=True,
            sep=";",
        )
        df = df.select(
            col("station_unlock").alias("unlock_id"),
            col("station_lock").alias("lock_id"),
            col("unlock_date").cast("date"),
            col("trip_minutes").alias("travel_time").cast("float"),
            col("unlocktype").cast("string"),
            col("geolocation_unlock")
        )
        df=df.withColumn("id",monotonically_increasing_id()+max_id+1)
        max_id=df.selectExpr("max(id)").collect()[0][0]
        df = df.select("id", *df.columns[:-1])
        #Añadimos la fecha para poder hacer una partición en S3.
        df = df.withColumn("zip_code", location_to_zipcode_dict_udf(col("geolocation_unlock")))
        df = df.drop("geolocation_unlock")
        df = df.withColumn("year", year("unlock_date")).withColumn("month", month("unlock_date"))
        df.write.mode("overwrite").partitionBy("year", "month").parquet("s3://curated-data-bicimad/trips/")
        

    elif name.endswith(".json"):
        df = spark.read.json(
            path=file_path,
        )
        df = df.select(
            col("idunplug_station").alias("unlock_id").cast("string"),
            col("idplug_station").alias("lock_id").cast("string"),
            col("unplug_hourTime").alias("unlock_date").cast("date"),
            col("travel_time").alias("travel_time").cast("float"),
            col("zip_code")
        )
        df=df.withColumn("unlocktype",lit("STATION"))
        df=df.withColumn("id",monotonically_increasing_id()+max_id+1)
        max_id=df.selectExpr("max(id)").collect()[0][0]
        df = df.select("id", *df.columns[:-1])
        df = df.withColumn("year", year("unlock_date")).withColumn("month", month("unlock_date"))
        df.write.mode("overwrite").partitionBy("year", "month").parquet("s3://curated-data-bicimad/trips/")


# COMMAND ----------

def location_to_zipcode_dict(dict_element):
    # Check if the input is a dictionary and has the 'coordinates' key
    if isinstance(dict_element, dict) and 'coordinates' in dict_element:
        print("dict")
        coordinates = dict_element['coordinates']
        # Assuming coordinates is a list that needs to be inverted
        inverted_coordinates = invertir_lista(coordinates)
        return location_to_zipcode(inverted_coordinates)
    # Check if the input is a string
    elif isinstance(dict_element, str):
        print("str")
        # Assuming the string is a comma-separated pair of coordinates
        coordinates = dict_element.split(',')
        if len(coordinates) == 2:
            inverted_coordinates = invertir_lista(coordinates)
            return location_to_zipcode(inverted_coordinates)
    print("None")
    return None
print(location_to_zipcode_dict("{'type': 'Point', 'coordinates': [-3.6751388, 40.4441388]}"))
# Register the function as a UDF
location_to_zipcode_dict_udf = udf(location_to_zipcode_dict, StringType())
df = spark.read.csv(
            path="s3://raw-data-bicimad/datos-bicimad/unzipped/trips_22_12_December.csv",
            header=True,
            sep=";",
        )
df = df.select(
            col("station_unlock").alias("unlock_id"),
            col("station_lock").alias("lock_id"),
            col("unlock_date").cast("date"),
            col("trip_minutes").alias("travel_time").cast("float"),
            col("unlocktype").cast("string"),
            col("geolocation_unlock")
        )
df = df.withColumn("zip_code", location_to_zipcode_dict_udf(col("geolocation_unlock")))
display(df)
