# -*- coding: utf-8 -*-
"""
Diplomado: Big data y análisis de datos con PySpark
Modulo: 1
Sesión: 03
Fecha: 03/06/2026
Tema: Principales objetos, estructuras y componentes de PySpark
Instructor: Alexis Adonai Morales Alberto
"""

# SparkSession 

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MiAplicacion") \
    .getOrCreate()
    
# DataFrame 

datos = [
    (1, "Ana", 25),
    (2, "Luis", 30)    
]

DF = spark.createDataFrame(datos, ["id", "Nombre", "Edad"])
DF.show()

## Manipulaciones básicas de un DataFrame con Spark 

DF.select("Nombre", "Edad").show()

## Filtro usando Spark 

DF.filter(DF.Edad > 25).show()

# RDD (Resilient Distributed Dataset)

rdd = spark.sparkContext.parallelize([1,2,3,4,5])

## Aplicar map 

rdd_squared = rdd.map(lambda x: x**2)
print(rdd_squared.collect())

pares = rdd.filter(lambda x: x % 2 == 0)
print(pares.collect())

impares = rdd.filter(lambda x: x % 2 != 0)
print(impares.collect())

# Columna 

DF["Edad"]

from pyspark.sql.functions import col
from Pyspark.sql.functions import col

DF2 = DF.withColumn("Edad_Doble", DF["Edad"] * 2)
DF2.show()

DF2 = DF.withColumn("Edad_Doble", col("Edad")*2)
DF2.show()

# Fila 

from pyspark.sql import Row

fila = Row(id = 1, Nombre = "Ana", Edad = 25)
print(fila.Nombre)

filas = [
    
    Row(id = 1, Nombre = "Ana", Edad = 25),
    Row(id = 2, Nombre = "Luis", Edad = 30)
    
]

DF = spark.createDataFrame(filas)
DF.show()

# Archivos de datos 

Titanic = spark.read.csv("Datos\\titanic.csv", 
                         header = True)

Titanic = spark.read.csv("C:\\Users\\alexi\\Downloads\\titanic.csv",
                         header = True)

Titanic.show()

# Conexión SQL 

Titanic.createOrReplaceTempView("titanic")

resultado = spark.sql("""
  SELECT Sexo, 
         ROUND(AVG(Age), 0) AS promedio_edad
  FROM titanic
  GROUP BY Sexo                                        
""")

resultado.show()
