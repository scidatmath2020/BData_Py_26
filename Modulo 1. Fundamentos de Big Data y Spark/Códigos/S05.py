# -*- coding: utf-8 -*-
"""
Diplomado: Big data y análisis de datos con PySpark
Modulo: 1
Sesión: 05
Fecha: 05/06/2026
Tema: Tipos de datos complejos, lectura csv y parquet
Instructor: Alexis Adonai Morales Alberto
"""

# Modulos requeridos para el proceso 

from pyspark.sql import SparkSession
from pyspark.sql.types import *


# SparkSession 

spark = SparkSession.builder \
    .appName("MiAplicacion") \
    .getOrCreate()
    
# Tipos de datos complejos 

## ArrayType 

schema = StructType([
    StructField("calificaciones", 
                ArrayType(IntegerType()),
                True)    
])

datos = [
    ([90, 85, 100],),
    ([70, 88, 95],)    
]

DF = spark.createDataFrame(datos, schema)
DF.show(truncate=False)
DF.printSchema()

## MapType (Clave-valor)

schema = StructType([
    StructField(
        "ventas",
        MapType(StringType(), IntegerType()),
        True   
        ) 
])

datos = [
    ({"enero": 1200, "febrero":1500},),
    ({"enero": 900, "febrero": 1100},)
]

DF = spark.createDataFrame(datos, schema)
DF.show(truncate=False)
DF.printSchema()

DF.select(DF["ventas"]["enero"].alias("Ventas_enero")).show()

# Lectura de datos csv a dataframe 

## Adicional: Extraer o descomprimir un zip 

### Rutas de acceso al archivo y donde se descomprimirá

archivo_zip = r"E:/SciData/PySpark y Big Data/BData_Py_26/Datos/enigh2024_ns_concentradohogar_csv.zip"
ruta_destino = r"E:/SciData/PySpark y Big Data/BData_Py_26/Datos/"

import zipfile

### Verificar los nombres de los archivo que estan comprimidos en el zip

with zipfile.ZipFile(archivo_zip, 'r') as zip_ref:
    print(zip_ref.namelist())

### Extracción del archivo especifico

with zipfile.ZipFile(archivo_zip, 'r') as zip_ref:
    zip_ref.extract("concentradohogar.csv",ruta_destino)

## Llamando con spark los datos csv 

ENIGH_csv = spark.read.csv(
        "Datos\\concentradohogar.csv",
        header = True,
        inferSchema=True
    )

ENIGH_csv.show()
ENIGH_csv.printSchema()

# Lectura de datos parquet a dataframe 

## Adicional: Extraer o descomprimir un zip 

### Rutas de acceso al archivo y donde se descomprimirá

archivo_zip = r"E:/SciData/PySpark y Big Data/BData_Py_26/Datos/enigh_concentradohogar_parquet.zip"
ruta_destino = r"E:/SciData/PySpark y Big Data/BData_Py_26/Datos/"

### Verificar los nombres de los archivo que estan comprimidos en el zip

with zipfile.ZipFile(archivo_zip, 'r') as zip_ref:
    print(zip_ref.namelist())

### Extracción del archivo especifico

with zipfile.ZipFile(archivo_zip, 'r') as zip_ref:
    zip_ref.extract("ENIGH.parquet",ruta_destino)
    
## Llamando con spark los datos parquet 

ENIGH_parquet = spark.read.parquet(
    "Datos\\ENIGH.parquet"
    ) 

ENIGH_parquet.show()
ENIGH_parquet.printSchema()











    


