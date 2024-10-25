# Databricks notebook source
import sys

sys.path.append('/Workspace/Users/pablobotet@gmail.com/Madrid_Project/common')
from util import read_xls_from_s3, get_file_keys_from_s3_folder, unzip_and_upload_files_to_s3

aws_access_key_id=dbutils.secrets.get("credentials", "AWS_user_id")
aws_secret_access_key=dbutils.secrets.get("credentials", "AWS_secret_access_key")
#En primer lugar tenemos que descomprimir los archivos. En primer lugar cogemos todas los archivos de la carpeta de s3 y los descomprimimos.

#Primero obtenemos todos los file_keys
file_key_list=get_file_keys_from_s3_folder('raw-data-bicimad', 'datos-bicimad/zipped', aws_access_key_id, aws_secret_access_key)
file_key_list=[i for i in file_key_list if '.zip' in i]
#Los descomprimimos y los almacenamos en otra carpeta. 
for file_key in file_key_list: 
    print(file_key)
    unzip_and_upload_files_to_s3(bucket_name='raw-data-bicimad', zip_file_key=file_key, target_s3_folder= 'datos-bicimad/unzipped', AWS_access_key_id=aws_access_key_id, AWS_secret_access_key =aws_secret_access_key)
    print(file_key)
#Los procesamos y los almacenamos en el bucket cleansed.

