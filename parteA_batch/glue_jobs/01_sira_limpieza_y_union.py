import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, year, month, broadcast
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)

# 1. Obtener parámetros de entrada
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "input_observaciones", "input_parcelas", "output_silver"]
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

input_obs = args["input_observaciones"]
input_parc = args["input_parcelas"]
output_silver = args["output_silver"]

print(f"=== INICIANDO JOB: {args['JOB_NAME']} ===")
print(f"Leyendo observaciones desde: {input_obs}")
print(f"Leyendo maestro parcelas desde: {input_parc}")
print(f"Escribiendo capa Silver en: {output_silver}")

# 2. Esquema explícito de las observaciones (optimización Spark - sin inferir esquema)
schema_obs = StructType([
    StructField("observacion_id", StringType(), False),
    StructField("parcela_id", StringType(), False),
    StructField("escena_id", StringType(), True),
    StructField("fecha", DateType(), True),
    StructField("satelite", StringType(), True),
    StructField("wrs_path", IntegerType(), True),
    StructField("wrs_row", IntegerType(), True),
    StructField("sr_b3_verde", DoubleType(), True),
    StructField("sr_b4_rojo", DoubleType(), True),
    StructField("sr_b5_nir", DoubleType(), True),
    StructField("sr_b6_swir1", DoubleType(), True),
    StructField("st_b10_termica", DoubleType(), True),
    StructField("qa_pixel", IntegerType(), True),
    StructField("qa_radsat", IntegerType(), True),
    StructField("sr_qa_aerosol", IntegerType(), True),
    StructField("nubes_escena_pct", DoubleType(), True),
    StructField("calidad_imagen", IntegerType(), True)
])

# 3. Lectura de datasets
df_obs_raw = spark.read.csv(input_obs, header=True, schema=schema_obs)
df_parcelas = spark.read.csv(input_parc, header=True, inferSchema=True)

# 4. Limpieza y validación de reglas de calidad espectral/agronómica
# R1: Descartar duplicados por observacion_id
df_dedup = df_obs_raw.dropDuplicates(["observacion_id"])

# R2: Filtrar filas con fecha nula o campos esenciales ausentes
df_valid_keys = df_dedup.filter(
    col("observacion_id").isNotNull() &
    col("parcela_id").isNotNull() &
    col("fecha").isNotNull()
)

# R3: Corrección/descarte de reflectancias físicamente imposibles (rango válido [0.0, 1.0])
df_clean_reflectance = df_valid_keys.filter(
    (col("sr_b3_verde") >= 0.0) & (col("sr_b3_verde") <= 1.0) &
    (col("sr_b4_rojo") >= 0.0) & (col("sr_b4_rojo") <= 1.0) &
    (col("sr_b5_nir") >= 0.0) & (col("sr_b5_nir") <= 1.0) &
    (col("sr_b6_swir1") >= 0.0) & (col("sr_b6_swir1") <= 1.0)
)

# 5. Cálculo de variables e índices biofísicos de SIRA
# NDMI = (NIR - SWIR1) / (NIR + SWIR1) -> Contenido hídrico en vegetación
# NDWI = (Verde - NIR) / (Verde + NIR) -> Agua superficial expuesta / lámina
# NDVI = (NIR - Rojo) / (NIR + Rojo)   -> Vigor vegetal
# LST_C = Temperatura de superficie en °C
df_indices = df_clean_reflectance.withColumn(
    "ndmi",
    (col("sr_b5_nir") - col("sr_b6_swir1")) / (col("sr_b5_nir") + col("sr_b6_swir1"))
).withColumn(
    "ndwi",
    (col("sr_b3_verde") - col("sr_b5_nir")) / (col("sr_b3_verde") + col("sr_b5_nir"))
).withColumn(
    "ndvi",
    (col("sr_b5_nir") - col("sr_b4_rojo")) / (col("sr_b5_nir") + col("sr_b4_rojo"))
).withColumn(
    "lst_c",
    when(
        col("st_b10_termica").isNotNull(),
        col("st_b10_termica") * 0.00341802 + 149.0 - 273.15
    ).otherwise(None)
).withColumn(
    "apta_optica",
    (col("qa_pixel").isin([21824, 21826])) &
    (col("nubes_escena_pct") < 70.0) &
    (col("calidad_imagen") >= 9)
).withColumn(
    "anio", year(col("fecha"))
).withColumn(
    "mes", month(col("fecha"))
)

# 6. Unión (JOIN) con el Maestro de Parcelas del Alto Magdalena
# Broadcast Join al ser df_parcelas una dimensión pequeña (< 10 MB)
# Se eliminan wrs_path y wrs_row del maestro para evitar columnas duplicadas al escribir en Parquet
df_parcelas_join = df_parcelas.drop("wrs_path", "wrs_row")

df_silver = df_indices.join(
    broadcast(df_parcelas_join),
    on="parcela_id",
    how="inner"
)

# 7. Escritura en Capa Silver en formato Parquet particionado por año
print(f"Escribiendo datos limpios y enriquecidos en {output_silver}...")
(
    df_silver.repartition("anio")
    .write
    .mode("overwrite")
    .partitionBy("anio")
    .parquet(output_silver)
)

print("=== JOB 01 COMPLETADO EXITOSAMENTE ===")
job.commit()
