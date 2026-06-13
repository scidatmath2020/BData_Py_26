# -*- coding: utf-8 -*-
"""
Diplomado: Big data y análisis de datos con PySpark
Modulo: 2
Sesión: 09
Fecha: 11/06/2026
Tema: Procesamiento de datos PT2
Instructor: Alexis Adonai Morales Alberto
"""

# Modulo a iniciar / cargar

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Iniciar con sesión de Spark

spark = SparkSession.builder\
    .appName("Incidencia_delictiva")\
    .getOrCreate()

# Carga de datos

INEFC = spark.read.csv(
    "/content/drive/MyDrive/Estatal-Delitos-2015-2025_abr2026.csv",
    header = True,
    inferSchema=True,
    encoding="utf-8"
    )

INEFC.show()
INEFC.printSchema()

INEFC_M26 = spark.read.csv(
    "/content/drive/MyDrive/RNID-Delitos_Estatal-2026-abr2026.csv",
    header = True,
    inferSchema=True,
    encoding="utf-8"
    )

INEFC_M26.show()
INEFC_M26.printSchema()

# Definir meses según número

mes_map = {
    "Enero": "01", "Febrero": "02", "Marzo": "03",
    "Abril": "04", "Mayo": "05",   "Junio": "06",
    "Julio": "07", "Agosto": "08", "Septiembre": "09",
    "Octubre": "10", "Noviembre": "11", "Diciembre": "12"
}

# Columnas de identificación de los datos no númericos

id_cols = INEFC.columns[:7]
month_cols = [c for c in INEFC.columns if c not in id_cols]

# Definir proceso de formato ancho a largo

def pivot_to_long(df, id_cols, month_cols, mes_map):
    """
    Esta definción equivale a realizar un proceso de pivoteo
    más creación de columna, más renombrar columnas y
    seleccionar columnas de interés. Además de transformar
    una columna en fecha según la estructura del tipo de dato (datetime)
    """

    # despivotar columnas de meses a filas

    stack_expr = ",".join([f"'{col}', `{col}`" for col in month_cols])
    df_long = df.selectExpr(
        *id_cols,
        f"stack({len(month_cols)}, {stack_expr}) as (Meses, Presuntos_delitos)"
        )

    # Mapear nombre del mes según numero de mes usando CASE WHEN

    mes_condicion = when(col("Meses") == "Enero", "01")\
                    .when(col("Meses") == "Febrero", "02")\
                    .when(col("Meses") == "Marzo", "03")\
                    .when(col("Meses") == "Abril", "04")\
                    .when(col("Meses") == "Mayo", "05")\
                    .when(col("Meses") == "Junio", "06")\
                    .when(col("Meses") == "Julio", "07")\
                    .when(col("Meses") == "Agosto", "08")\
                    .when(col("Meses") == "Septiembre", "09")\
                    .when(col("Meses") == "Octubre", "10")\
                    .when(col("Meses") == "Noviembre", "11")\
                    .when(col("Meses") == "Diciembre", "12")

    df_long = df_long \
        .withColumn("Num_mes", mes_condicion)\
        .withColumn("Fecha",
                    to_date(concat_ws("-", col("Anio").cast("string"), col("Num_mes"), lit("01")),
                            "yyyy-MM-dd"))\
            .drop("Meses", "Num_mes", "Anio")

    return df_long

# Cambio de formato

INEFC_long = pivot_to_long(INEFC, id_cols, month_cols, mes_map)
INEFC_long.show()

# Cambio de formato para metodología 2026

## NOTA: Como las columnas de mayo a diciembre contienen NULL
## pyspark las va a leer como str, entonces hay que forzar
## a que sean int

for mes in month_cols:
    INEFC_M26 = INEFC_M26.withColumn(mes, col(mes).cast("int"))

INEFC_M26.printSchema()

INEFC_M26_long = pivot_to_long(INEFC_M26, id_cols, month_cols, mes_map)\
    .dropna()

INEFC_M26_long.show()

# Unión de datos

Base = INEFC_long.union(INEFC_M26_long)
Base.printSchema()
Base.show()

# Escritura de datos

Base.coalesce(1).write\
    .option("header", "true")\
    .option("encoding", "utf-8")\
    .mode("overwrite")\
    .csv("/content/drive/MyDrive/Carpeta_Salida_IDFCE")

# Escritura de datos

Base.coalesce(2).write\
    .option("header", "true")\
    .option("encoding", "utf-8")\
    .mode("overwrite")\
    .csv("/content/drive/MyDrive/Carpeta_Salida_IDFCE_2pt")

Base.count()

# ¿Cómo hacerlo con pandas?

Base.toPandas().to_csv(
        "/content/drive/MyDrive/IDEFC_2015_2026_ABR.csv",
        index=False,
        encoding="utf-8"
    )

# Crear tabla de datos de la población nacional por año

POB = spark.read.csv(
    "/content/drive/MyDrive/00_Pob_Mitad_1950_2070.csv",
    header = True,
    inferSchema=True
)

POB.printSchema()

POB.show()

Poblacion_anio = POB.groupby("FECHA").agg(
    sum("POBLACION").alias("Poblacion")
).withColumnRenamed("FECHA", "Anio")

Poblacion_anio = Poblacion_anio.withColumn("Anio", year(col("Anio")))

Poblacion_anio.orderBy("Anio").show()

# Crear suma agrupada de algun delito de interés
# La suma será agrupada nacional por año

## Revisar delitos únicos

## Tipos de delitos

Base.select("Tipo_de_delito").distinct().show(100,truncate=False)

## Subtipo de delito

Base.select("Subtipo_de_delito").distinct().show(100,truncate=False)

Base.filter(year(col("Fecha")) !=2026)\
    .select("Tipo_de_delito", "Subtipo_de_delito")\
    .distinct()\
    .orderBy("Tipo_de_delito", "Subtipo_de_delito")\
    .show(100,truncate=False)

## Sumas agrupadas por año

Base_anio = Base.filter(year(col("Fecha")) !=2026)\
                .withColumn("Anio", year(col("Fecha")))\
                .groupBy("Anio", "Tipo_de_delito", "Subtipo_de_delito")\
                .agg(sum("Presuntos_delitos").alias("Total_delitos"))\
                .orderBy("Anio", "Tipo_de_delito", "Subtipo_de_delito")

Base_anio = Base_anio.filter((col("Tipo_de_delito")== "Robo") &
                        (col("Subtipo_de_delito") == "Robo a transeúnte en vía pública"))

Base_anio.show(truncate=False)

# Unión de datos para cálcular tasa de cada 100 mil

Base_anio_pob = Base_anio.join(Poblacion_anio, on = "Anio", how = "left")

Base_anio_pob.orderBy("Anio").show(truncate=False)

Base_anio_pob = Base_anio_pob\
                .withColumn("Tasa_100k",
                            (col("Total_delitos")/col("Poblacion"))*100000)\
                .orderBy("Anio")

Base_anio_pob.show(truncate=False)

# Guardamos salida final

Base_anio_pob.toPandas().to_csv(
        "/content/drive/MyDrive/Robo_tramseunte_VP_2015_2015.csv",
        index=False,
        encoding="utf-8"
    )

spark.stop()

import pandas as pd

Datos = pd.read_csv("/content/drive/MyDrive/Robo_tramseunte_VP_2015_2015.csv")

Datos

