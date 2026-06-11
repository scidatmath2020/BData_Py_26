# -*- coding: utf-8 -*-
"""
Diplomado: Big data y análisis de datos con PySpark
Modulo: 2
Sesión: 06
Fecha: 08/06/2026
Tema: Filtros y agrupaciones
Instructor: Alexis Adonai Morales Alberto
"""

# Modulos a importar 

from pyspark.sql import SparkSession 
from pyspark.sql import *

# Crear la sesión de Spark 

spark = SparkSession.builder \
        .appName("Filtros") \
        .getOrCreate()

# Lectura y conversión de la encuesta a DataFrame 

ENIGH = spark.read.csv("Datos\\concentradohogar.csv",
                       header = True)

ENIGH.show()
ENIGH.printSchema()

# Modificación de columna según tipo de dato 

## Pasar educa_jefe a número sabiendo que es cadena 

from pyspark.sql.functions import col, regexp_replace, trim
from pyspark.sql.types import DoubleType, IntegerType

ENIGH = ENIGH.withColumn(
     "edad_jefe", col("edad_jefe").cast(DoubleType())
    )

ENIGH.printSchema()

# Ejemplo de como limpiar la columna 

ENIGH = ENIGH.withColumn(
    "edad_jefe",
    regexp_replace(trim(col("edad_jefe")), r"[$,]", "").cast("double")                     
    )

# Filtros 

## Con variable numérica (edad_jefe)

ENIGH.count()

### Refiriendo a la columna desde el DataFrame 

ENIGH.filter(ENIGH["edad_jefe"] > 40).show()
ENIGH.filter(ENIGH["edad_jefe"] > 40).count()

### Refiriendo mediante col 

ENIGH.filter(col("edad_jefe") > 40).show()
ENIGH.filter(col("edad_jefe") > 40).count()

### Realizar filtro con vista temporal y SQL 

ENIGH.createOrReplaceTempView("ENIGH")

ENIGH1 = spark.sql("SELECT * FROM ENIGH WHERE edad_jefe > 40")
ENIGH1.count()

# Filtros con valores exactos 

## Obtener datos de jefes de familia con edades iguales a  15 años

ENIGH.filter(col("edad_jefe") == 15) \
     .select("edad_jefe", "educa_jefe", "sexo_jefe") \
     .show()

spark.sql("SELECT edad_jefe, educa_jefe, sexo_jefe FROM ENIGH WHERE edad_jefe = 15").show()

# Filtros con valores mayores que 

## Obtener datos de jefes de familia con edades mayores a 15 años

ENIGH.filter(col("edad_jefe") > 15) \
     .select("edad_jefe", "educa_jefe", "sexo_jefe") \
     .show()
     
     
# Filtros con valores menores que 

## Obtener datos de jefes de familia con edades menores a 25 años

ENIGH.filter(col("edad_jefe") < 25) \
     .select("edad_jefe", "educa_jefe", "sexo_jefe") \
     .show()

# Filtros con valores mayores o iguales que

## Obtener datos de jefes de familia con edades mayores o iguales que 60 años

ENIGH.filter(col("edad_jefe") >= 60) \
     .select("edad_jefe", "educa_jefe", "sexo_jefe") \
     .show()

# Filtros con valores menores o iguales que

## Obtener datos de jefes de familia con edades menores o iguales que 17 años

ENIGH.filter(col("edad_jefe") <= 17) \
     .select("edad_jefe", "educa_jefe", "sexo_jefe") \
     .show()

# Filtros usando el operador Y (AND &)

ENIGH.filter((col("edad_jefe") <= 17) & (col("sexo_jefe") == "1"))\
    .select("edad_jefe", "educa_jefe", "sexo_jefe")\
    .show()

# Filtros de "o" (OR |)

ENIGH.filter((col("edad_jefe") == 25) | (col("educa_jefe") == "07"))\
    .select("edad_jefe", "educa_jefe", "sexo_jefe")\
    .show()

# Filtros isin (dentro de)

ENIGH.filter(col("edad_jefe").isin([17,18,19]))\
    .select("edad_jefe", "educa_jefe", "sexo_jefe")\
    .show()

ENIGH.filter((col("edad_jefe").isin([17,18,19])) & col("educa_jefe").isin(["08", "09"]))\
    .select("edad_jefe", "educa_jefe", "sexo_jefe")\
    .show()
    
# Filtros between (rangos)

ENIGH.filter(col("edad_jefe").between(18,20))\
    .select("edad_jefe", "educa_jefe", "sexo_jefe")\
    .show()

# Filtros con patrones de texto 

INFCE = spark.read.csv("Datos\\INDFCE_PD_2015-2026abr.csv",
                       header = True,
                       inferSchema=True,
                       encoding="utf-8")


INFCE.show()
INFCE.printSchema()

INFCE.filter((col("Subtipo de delito") == "Homicidio doloso") & (col("Modalidad").like("Con arma%")))\
    .select("Fecha", "Clave_Ent", "Subtipo de delito", "Modalidad", "Delitos")\
    .show()
    

INFCE.filter((col("Subtipo de delito") == "Homicidio doloso") & (col("Modalidad").rlike(r".*arma")))\
    .select("Fecha", "Clave_Ent", "Subtipo de delito", "Modalidad", "Delitos")\
    .show()


