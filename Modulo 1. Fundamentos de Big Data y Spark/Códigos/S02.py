# -*- coding: utf-8 -*-
"""
Diplomado: Big data y análisis de datos con PySpark
Modulo: 1
Sesión: 02
Fecha: 02/06/2026
Tema: Mi primer conexión con Spark en Python
Instructor: Alexis Adonai Morales Alberto
"""

import os
os.environ["PYSPARK_PYTHON"] = r"C:\Program Files\Python312\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Program Files\Python312\python.exe"

# Procedimiento que se tiene que repetir cada clase 

from pyspark.sql import SparkSession

# Crear la sesión de Spark 

spark = SparkSession.builder \
    .appName("MiAppSpark") \
    .master("local[*]") \
    .getOrCreate()

# Probar si la sesión se efectuo 

print(spark.version)

# Crear un DF de prueba 

data = [("Ana", 25), ("Luis", 30), ("Marta", 20)]
columns = ["Nombre", "Edad"]

DF = spark.createDataFrame(data, columns)
DF.show()

# Cerrar sesión 
spark.stop()





