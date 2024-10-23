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

# Now, modify your util.py function to accept AWS credentials as parameters
# Assuming you've done that, call the function with the credentials
district_xls = read_xls_from_s3(
    'prueba-acceso',
    'D01T0123.xls',
    AWS_access_key_id=aws_access_key_id,
    AWS_secret_access_key=aws_secret_access_key
)
