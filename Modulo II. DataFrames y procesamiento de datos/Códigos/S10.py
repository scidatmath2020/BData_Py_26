# -*- coding: utf-8 -*-
"""
Diplomado: Big data y análisis de datos con PySpark
Modulo: 2
Sesión: 10
Fecha: 15/06/2026
Tema: Procesamiento de datos usando SQL
Instructor: Alexis Adonai Morales Alberto
"""

# Modulos a importar

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Iniciar sesión de Spark

spark = SparkSession.builder\
      .appName("Incidencia_delictiva_SQL")\
      .getOrCreate()

# 2. Carga de datos y creación de vista temporal

## 2.1 Tabla de datos de IDFEC 2015-2025

INEFC = spark.read.csv(
    "/content/drive/MyDrive/Estatal-Delitos-2015-2025_abr2026.csv",
    header=True,
    inferSchema=True,
    encoding="utf-8"
)

INEFC.createOrReplaceTempView("INEFC")

spark.sql(f"""
  SELECT * FROM INEFC
""").show()

## 2.2 Tabla de datos de IDFEC Met 2026 Enero-Abril

INEFC_M26 = spark.read.csv(
    "/content/drive/MyDrive/RNID-Delitos_Estatal-2026-abr2026.csv",
    header=True,
    inferSchema=True,
    encoding="utf-8"
)

## Forzar las columnas int para evitar problemas

id_cols = INEFC.columns[:7]
month_cols = [c for c in INEFC.columns if c not in id_cols]

for mes in month_cols:
  INEFC_M26 = INEFC_M26.withColumn(mes, col(mes).cast("int"))

INEFC_M26.createOrReplaceTempView("INEFC_M26")

spark.sql(f"""
  SELECT * FROM INEFC_M26
""").show()

## 2.3 Tabla de Población

POB = spark.read.csv(
    "/content/drive/MyDrive/00_Pob_Mitad_1950_2070.csv",
    header=True,
    inferSchema=True
)

POB.createOrReplaceTempView("POB")

spark.sql(f"""
  SELECT * FROM POB
""").show()

# 3. Pivoteo de los datos de incidencia delictiva

## 3.1 Columnas de identificación

id_cols_sql = ", ".join(id_cols)
id_cols_sql

## 3.2 Contruir la expresión STACK

stack_pairs = ",".join([f"'{m}', `{m}`" for m in month_cols])
stack_expr = f"stack({len(month_cols)}, {stack_pairs}) AS (Meses, Presuntos_delitos)"

## 3.3 pivoteo de INEFC

INEFC_long = spark.sql(f"""
  SELECT
    {id_cols_sql},
    {stack_expr}
  FROM INEFC
""")

INEFC_long.createOrReplaceTempView("INEFC_long")

spark.sql(f"""
  SELECT * FROM INEFC_long
""").show()

## 3.4 pivoteo de INEFC_M26

INEFC_M26_long = spark.sql(f"""
  SELECT
    {id_cols_sql},
    {stack_expr}
  FROM INEFC_M26
""")

INEFC_M26_long.createOrReplaceTempView("INEFC_M26_long")

spark.sql(f"""
  SELECT * FROM INEFC_M26_long
""").show()

# 4. Crear columna de fecha apartir del año y del mes

def sql_mes_a_fecha(vista_raw: str) -> str:
  return f"""
  SELECT
        * EXCEPT (Meses, Anio),
        TO_DATE(
            CONCAT_WS('-',
                CAST(Anio AS STRING),
                CASE Meses
                    WHEN 'Enero'      THEN '01'
                    WHEN 'Febrero'    THEN '02'
                    WHEN 'Marzo'      THEN '03'
                    WHEN 'Abril'      THEN '04'
                    WHEN 'Mayo'       THEN '05'
                    WHEN 'Junio'      THEN '06'
                    WHEN 'Julio'      THEN '07'
                    WHEN 'Agosto'     THEN '08'
                    WHEN 'Septiembre' THEN '09'
                    WHEN 'Octubre'    THEN '10'
                    WHEN 'Noviembre'  THEN '11'
                    WHEN 'Diciembre'  THEN '12'
                END,
                '01'
            ),
            'yyyy-MM-dd'
        ) AS Fecha
    FROM {vista_raw}
  """

INEFC_long_fecha = spark.sql(sql_mes_a_fecha("INEFC_long"))
INEFC_long_fecha.show()

INEFC_M26_long_fecha = spark.sql(sql_mes_a_fecha("INEFC_M26_long"))\
                            .dropna()
INEFC_M26_long_fecha.show()

INEFC_long_fecha.createOrReplaceTempView("INEFC_long_fecha")
INEFC_M26_long_fecha.createOrReplaceTempView("INEFC_M26_long_fecha")

# 5. Unión de las tablas "temporales o de vista temporal"

Base = spark.sql("""
  SELECT * FROM INEFC_long_fecha
  UNION ALL
  SELECT * FROM INEFC_M26_long_fecha
""")

Base.createOrReplaceTempView("Base")

Base.printSchema()

Base.show(truncate = False)

# 6. Crear consulta para tabla de población

Poblacion_anio = spark.sql("""
  SELECT
    YEAR(FECHA) AS Anio,
    SUM(POBLACION) AS Poblacion
  FROM POB
  GROUP BY YEAR(FECHA)
  ORDER BY Anio
""")

Poblacion_anio.createOrReplaceTempView("Poblacion_anio")
Poblacion_anio.show()

# 7. Exploración de delitos únicos

spark.sql("""
  SELECT DISTINCT Tipo_de_delito
  FROM Base
  ORDER BY Tipo_de_delito
""").show(100, truncate = False)

spark.sql("""
  SELECT DISTINCT Subtipo_de_delito
  FROM Base
  ORDER BY Subtipo_de_delito
""").show(100, truncate = False)

spark.sql("""
  SELECT DISTINCT
    Tipo_de_delito,
    Subtipo_de_delito
  FROM Base
  WHERE YEAR(Fecha) != 2026
  ORDER BY Tipo_de_delito, Subtipo_de_delito
""").show(100, truncate = False)

# 8. Suma anual agrupada

Base_anio = spark.sql("""
  SELECT
    YEAR(Fecha) AS Anio,
    Tipo_de_delito,
    Subtipo_de_delito,
    SUM(Presuntos_delitos) AS Total_delitos
  FROM Base
  WHERE YEAR(Fecha) != 2026
    AND Tipo_de_delito = 'Robo'
    AND Subtipo_de_delito = 'Robo a transeúnte en vía pública'
  GROUP BY
    YEAR(Fecha),
    Tipo_de_delito,
    Subtipo_de_delito
  ORDER BY Anio
""")

Base_anio.createOrReplaceTempView("Base_anio")
Base_anio.show(truncate = False)

# 9. Unión de total de delitos anuales (según el tipo y subtipo)
#    y cálculo de presuntos delitos por cada 100,000 personas

Base_anio_pob =spark.sql("""
  SELECT
    b.Anio,
    b.Tipo_de_delito,
    b.Subtipo_de_delito,
    b.Total_delitos,
    p.Poblacion,
    (b.Total_delitos/p.Poblacion) * 100000 AS Tasa_100k
  FROM Base_anio b
  LEFT JOIN Poblacion_anio p
    ON b.Anio = p.Anio
  ORDER BY b.Anio
""")

Base_anio_pob.show(truncate=False)

# 10. Exportar resultado final con pandas

Base_anio_pob.toPandas().to_csv(
    "/content/drive/MyDrive/Robo_transeute_VP_2015_2025.csv",
    index=False,
    encoding="utf-8"
)

spark.stop()

