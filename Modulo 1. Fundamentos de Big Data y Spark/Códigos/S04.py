# -*- coding: utf-8 -*-
"""
Diplomado: Big data y análisis de datos con PySpark
Modulo: 1
Sesión: 04
Fecha: 04/06/2026
Tema: Principales tipos de datos
Instructor: Alexis Adonai Morales Alberto
"""

# SparkSession 

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MiAplicacion") \
    .getOrCreate()
    
# Crear estructura con tipos de objetos numéricos

from pyspark.sql.types import *
from decimal import Decimal

schema = StructType([
    StructField("byte", ByteType(), True),
    StructField("short", ShortType(), True),
    StructField("integer", IntegerType(),True),
    StructField("long", LongType() ,True),
    StructField("float", FloatType(), True),
    StructField("double", DoubleType() ,True),
    StructField("decimal", DecimalType(10,2) ,True)    
])

datos = [
    (127, 32000, 100000, 1000000000, 3.14, 3.14459265358979, Decimal("12345.67"))
]

df = spark.createDataFrame(datos, schema)
df.show()
df.printSchema()

# Tipo de cadena 

schema = StructType([
    StructField("nombre", StringType(), True)
])

datos = [
    ("Ana",),
    ("Carlos",),
    ("María",)
]

df = spark.createDataFrame(datos, schema)

df.show()
df.printSchema()

# Tipo binario 

schema = StructType([
    StructField("archivo", BinaryType(), True)
])

datos = [
    (bytearray(b"Hola"),),
    (bytearray(b"Spark"),)
]

df = spark.createDataFrame(datos, schema)

df.show(truncate=False)
df.printSchema()

# Tipo booleano 

schema = StructType([
    StructField("aprobado", BooleanType(), True)
])

datos = [
    (True,),
    (False,),
    (True,)
]

df = spark.createDataFrame(datos, schema)

df.show()
df.printSchema()

# Tipo fecha 

from datetime import date

schema = StructType([
    StructField("fecha", DateType(), True)
])

datos = [
    (date(2026, 6, 4),),
    (date(2026, 12, 25),)
]

df = spark.createDataFrame(datos, schema)

df.show()
df.printSchema()

# Tipo fecha con hora 

from datetime import datetime

schema = StructType([
    StructField("momento", TimestampType(), True)
])

datos = [
    (datetime(2026, 6, 4, 18, 30, 0),),
    (datetime(2026, 6, 5, 9, 15, 0),)
]

df = spark.createDataFrame(datos, schema)

df.show(truncate=False)
df.printSchema()
