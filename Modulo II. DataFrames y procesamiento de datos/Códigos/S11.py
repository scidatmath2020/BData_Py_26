# -*- coding: utf-8 -*-
"""
Diplomado: Big data y análisis de datos con PySpark
Modulo: 2
Sesión: 11
Fecha: 16/06/2026
Tema: UDFs y funciones nativas
Instructor: Alexis Adonai Morales Alberto
"""

# Modulos y clases a utilizar

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField

# Crear sesión de Spark

spark = (SparkSession.builder
         .appName("UDF_vs_nativas")
         .master("local[*]")
         .getOrCreate())

# Crear data frame de prueba

datos = [
    (" Ana García ", "CDMX"),
    ("LUIS MARTÍNEZ", "Monterrey"),
    (" pedro López", "Guadalajara"),
    ("SOFÍA RAMÍREZ", "CDMX"),
    (" jorge Silva", "Monterrey"),
]

schema = StructType([
    StructField("nombre", StringType(), True),
    StructField("ciudad", StringType(), True)
])

df = spark.createDataFrame(datos, schema)
df.show()

# Proceso para limpiar el formato de nombres (usando UDF Python)

@udf(StringType())
def limpiar_udf(texto):
  if texto is None:
    return None
  return texto.strip().lower()

df_udf = df.withColumn("nombre_limpio_udf", limpiar_udf(F.col("nombre")))

df_udf.show(truncate=False)

# Proceso para limpiar el formato de nombres (programación nativa)

df_nativa = df.withColumn("nombre_limpio_nativo", F.trim(F.lower(F.col("nombre"))))

df_nativa.show(truncate=False)

# Plan de ejecución de UDF

print("== Plan con UDF (Catalyst no puede optimizar) ==")
df_udf.select("nombre_limpio_udf").explain()

# Plan de ejecución con funciones nativas

print("== Plan con nativas (Catalyst optimiza) ==")
df_nativa.select("nombre_limpio_nativo").explain()

