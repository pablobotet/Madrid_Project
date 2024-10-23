# Databricks notebook source
import sys
sys.path.append('/Workspace/Users/pablobotet@gmail.com/Madrid_Project/common')
from util import read_xls_from_s3

#Ahora tenemos que obtener los datos del xls de los distritos

df=read_xls_from_s3('prueba-acceso','D01T0123.xlsx')
