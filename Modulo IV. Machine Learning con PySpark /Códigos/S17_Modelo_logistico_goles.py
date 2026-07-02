# -*- coding: utf-8 -*-
"""
Created on Fri Jun 26 17:23:56 2026

@author: alexi
"""

# Llamado de modulos

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler
)

from pyspark.ml.classification import LogisticRegression

from pyspark.ml import Pipeline

from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)

# Crear sesión de spark

spark = (
    SparkSession
    .builder
    .appName("Skew_Cache")
    .getOrCreate()
)

# Cargar datos parquet 

Partidos = spark.read.parquet(
    "Datos\\partidos.parquet"
    )

Partidos.show()

# Paso 1 filtrado 

Partidos.columns

shots = (
    Partidos
    .filter(col("`type.name`") == "Shot")
    )

print("Número de disparos:", shots.count())

###############################################################
# 2. VARIABLE RESPUESTA
###############################################################

shots = shots.withColumn(
    "goal",
    when(
        col("`shot.outcome.name`") == "Goal",
        1
    ).otherwise(0)
)

shots.groupBy("goal").count().show()

###############################################################
# 6. EXTRAER COORDENADAS
###############################################################

shots = (
    shots
    .withColumn("x", col("location")[0])
    .withColumn("y", col("location")[1])
)

###############################################################
# 7. DISTANCIA AL CENTRO DE LA PORTERÍA
###############################################################

shots = shots.withColumn(
    "distance",
    sqrt(
        pow(120-col("x"),2) +
        pow(40-col("y"),2)
    )
)

###############################################################
# 8. DISTANCIA LATERAL
###############################################################

shots = shots.withColumn(
    "y_center",
    abs(col("y")-40)
)

###############################################################
# 9. ÁNGULO APROXIMADO
###############################################################

shots = shots.withColumn(
    "angle",
    atan(
        lit(7.32)/col("distance")
    )
)

###############################################################
# 10. VARIABLES BINARIAS
###############################################################

shots = shots.withColumn(
    "pressure",
    when(col("under_pressure")==True,1).otherwise(0)
)

shots = shots.withColumn(
    "first_time",
    when(col("`shot.first_time`")==True,1).otherwise(0)
)

shots = shots.withColumn(
    "one_on_one",
    when(col("`shot.one_on_one`")==True,1).otherwise(0)
)

shots = shots.withColumn(
    "open_goal",
    when(col("`shot.open_goal`")==True,1).otherwise(0)
)

###############################################################
# 11. ELIMINAR OBSERVACIONES SIN UBICACIÓN
###############################################################

shots = shots.dropna(
    subset=[
        "x",
        "y",
        "distance",
        "angle"
    ]
)

###############################################################
# VARIABLES DEL MODELO
###############################################################

modelo_df = (
    shots.select(
        "goal",
        "`player.name`",
        "`team.name`",
        "minute",
        "distance",
        "angle",
        "x",
        "y",
        "pressure",
        "first_time",
        "one_on_one",
        "open_goal",
        "`shot.body_part.name`",
        "`shot.technique.name`",
        "`play_pattern.name`"
    )
    .withColumnRenamed("player.name", "player")
    .withColumnRenamed("team.name", "team")
    .withColumnRenamed("shot.body_part.name", "body_part")
    .withColumnRenamed("shot.technique.name", "technique")
    .withColumnRenamed("play_pattern.name", "play_pattern")
)

###############################################################
# 12. VARIABLES CATEGÓRICAS
###############################################################

body_indexer = StringIndexer(
    inputCol="body_part",
    outputCol="body_index",
    handleInvalid="keep"
)

tech_indexer = StringIndexer(
    inputCol="technique",
    outputCol="tech_index",
    handleInvalid="keep"
)

pattern_indexer = StringIndexer(
    inputCol="play_pattern",
    outputCol="pattern_index",
    handleInvalid="keep"
)

###############################################################
# 13. ONE HOT ENCODER
###############################################################

encoder = OneHotEncoder(
    inputCols=[
        "body_index",
        "tech_index",
        "pattern_index"
    ],
    outputCols=[
        "body_vec",
        "tech_vec",
        "pattern_vec"
    ]
)

###############################################################
# 14. VARIABLES DEL MODELO
###############################################################

assembler = VectorAssembler(
    inputCols=[
        "distance",
        "angle",
        "x",
        "y",
        "pressure",
        "first_time",
        "one_on_one",
        "open_goal",
        "body_vec",
        "tech_vec",
        "pattern_vec"
    ],
    outputCol="features"
)

###############################################################
# 15. MODELO
###############################################################

lr = LogisticRegression(
    featuresCol="features",
    labelCol="goal",
    predictionCol="prediction",
    probabilityCol="probability",
    maxIter=100
)

###############################################################
# 16. PIPELINE
###############################################################

pipeline = Pipeline(
    stages=[
        body_indexer,
        tech_indexer,
        pattern_indexer,
        encoder,
        assembler,
        lr
    ]
)

###############################################################
# 17. TRAIN / TEST
###############################################################

train, test = modelo_df.randomSplit(
    [0.80, 0.20],
    seed=123
)

###############################################################
# VALIDACIÓN
###############################################################

print("="*60)
print("CONJUNTO DE ENTRENAMIENTO")
print("="*60)

print(f"Entrenamiento : {train.count()}")
print(f"Prueba        : {test.count()}")

print("\nVariables:")

for c in train.columns:
    print(c)

###############################################################
# 18. ENTRENAMIENTO
###############################################################

model = pipeline.fit(train)

###############################################################
# 19. PREDICCIONES
###############################################################

pred = model.transform(test)

###############################################################
# 20. OBTENER xG
###############################################################

from pyspark.sql.functions import col
from pyspark.ml.functions import vector_to_array


pred = pred.withColumn(
    "xG",
    vector_to_array(col("probability"))[1]
)

###############################################################
# 21. RESULTADOS
###############################################################

pred.select(
    "player",
    "distance",
    "angle",
    "goal",
    "prediction",
    "xG"
).show(30, False)

###############################################################
# 22. MÉTRICAS
###############################################################

from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)

auc = BinaryClassificationEvaluator(
    labelCol="goal",
    metricName="areaUnderROC"
).evaluate(pred)

accuracy = MulticlassClassificationEvaluator(
    labelCol="goal",
    predictionCol="prediction",
    metricName="accuracy"
).evaluate(pred)

precision = MulticlassClassificationEvaluator(
    labelCol="goal",
    predictionCol="prediction",
    metricName="weightedPrecision"
).evaluate(pred)

recall = MulticlassClassificationEvaluator(
    labelCol="goal",
    predictionCol="prediction",
    metricName="weightedRecall"
).evaluate(pred)

f1 = MulticlassClassificationEvaluator(
    labelCol="goal",
    predictionCol="prediction",
    metricName="f1"
).evaluate(pred)

###############################################################
# 23. IMPRIMIR MÉTRICAS
###############################################################

print("\n")
print("="*60)
print("RESULTADOS DEL MODELO")
print("="*60)

print(f"AUC ROC   : {auc:.4f}")
print(f"Accuracy  : {accuracy:.4f}")
print(f"Precision : {precision:.4f}")
print(f"Recall    : {recall:.4f}")
print(f"F1 Score  : {f1:.4f}")

###############################################################
# 24. COEFICIENTES
###############################################################

lr_model = model.stages[-1]

print("\nIntercepto")
print(lr_model.intercept)

print("\nCoeficientes")
print(lr_model.coefficients)

###############################################################
# 25. VARIABLES MÁS IMPORTANTES
###############################################################

print("\nNúmero de coeficientes:")
print(len(lr_model.coefficients))

###############################################################
# 26. DISPAROS CON MAYOR xG
###############################################################

pred.select(
    "player",
    "team",
    "minute",
    "distance",
    "angle",
    "goal",
    "prediction",
    "xG"
).orderBy(
    col("xG").desc()
).show(20, False)

###############################################################
# 27. MATRIZ DE CONFUSIÓN
###############################################################

pred.groupBy(
    "goal",
    "prediction"
).count().orderBy(
    "goal",
    "prediction"
).show()

###############################################################
# 28. GUARDAR RESULTADOS
###############################################################

pred.select(
    "player",
    "team",
    "minute",
    "goal",
    "prediction",
    "xG"
).write.mode("overwrite").parquet("Resultados_xG")