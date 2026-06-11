# -*- coding: utf-8 -*-
"""
Diplomado: Big data y análisis de datos con PySpark
Modulo: 2
Sesión: 07
Fecha: 09/06/2026
Tema: Operaciones agrupadas
Instructor: Alexis Adonai Morales Alberto
"""

# Crear sesión de PySpark

from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName('Agrupaciones')\
        .getOrCreate()

# Importar encuesta como dataframe

ENIGH = spark.read.csv(
  "/content/drive/MyDrive/concentradohogar.csv",
  inferSchema=True,
  header=True
)

ENIGH.printSchema()

# Recodificación de variables

from pyspark.sql.functions import *

## Con valores exactamente igual a

ENIGH = ENIGH.withColumn("sexo_jefe",
                         when(col("sexo_jefe") == 1, "Hombre")
                         .when(col("sexo_jefe") == 2, "Mujer")
                         .otherwise("No identificado"))

ENIGH.select("edad_jefe", "sexo_jefe", "educa_jefe").show()

## Usando múltiples valores en la recodificación

ENIGH=ENIGH.withColumn("educa_jefe",
                 when(col("educa_jefe").isin([4,6]), "Básica completa")
                 .when(col("educa_jefe").isin([3,5]), "Básica incompleta")
                 .when(col("educa_jefe") == 8, "Media superior completa")
                 .when(col("educa_jefe") == 7, "Media superior incompleta")
                 .when(col("educa_jefe").isin([10,11]), "Superior completa")
                 .when(col("educa_jefe") == 9, "Superior incompleta")
                 .otherwise("Otro"))

ENIGH.select("edad_jefe", "sexo_jefe", "educa_jefe").show()

ENIGH.select("edad_jefe", "sexo_jefe", "educa_jefe")\
     .drop("educa_jefe")\
     .show()

# Operaciones agrupadas

## Suma básica

ENIGH.groupBy("sexo_jefe").agg(
    sum("factor").alias("Personas")
).show()

## Conteo
ENIGH.groupBy("sexo_jefe").agg(
    count("*").alias("Personas")
).show()

## Operaciones internas

ENIGH.groupby("sexo_jefe").agg(
    ((sum(col("edad_jefe")*col("factor")*col("tot_integ")))/(sum(col("factor")*col("tot_integ"))))\
    .alias("Edad_promedio")
).show()

# Operaciones agrupadas

## Suma básica

ENIGH.groupBy("sexo_jefe").agg(
    sum(col("factor")*col("mayores")).alias("Personas")
).show()

ENIGH.agg(
    (sum(col("factor")*col("hombres"))).alias("Hombres")
).show()

ENIGH.agg(
    (sum(col("factor")*col("mujeres"))).alias("Mujeres")
).show()

## Suma agrupada de los niveles educativos según el sexo del jefe de hogar

ENIGH.groupby("sexo_jefe", "educa_jefe").agg(
    sum(col("factor")).alias("Viviendas")
).show()

## Suma agrupada con dos variables y pivoteo a formato ancho

ENIGH.groupby("educa_jefe").pivot("sexo_jefe").agg(
    sum(col("factor"))
).show()

## Suma agrupada con dos variables y pivoteo a formato ancho
## Crear columna del total (H+M)

ENIGH.groupby("educa_jefe").pivot("sexo_jefe").agg(
    sum(col("factor"))
).withColumn(
        "Total",
        col("Hombre")+col("Mujer")).show()

ENIGH.agg(
    sum(col("factor"))
).show()