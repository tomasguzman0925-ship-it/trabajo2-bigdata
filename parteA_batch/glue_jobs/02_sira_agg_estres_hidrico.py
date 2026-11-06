import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, avg, sum, when, round

# 1. Obtener parámetros de entrada
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "input_silver", "output_gold_estres"]
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

input_silver = args["input_silver"]
output_gold = args["output_gold_estres"]

print(f"=== INICIANDO JOB RAMA 1: {args['JOB_NAME']} ===")
print(f"Leyendo capa Silver desde: {input_silver}")
print(f"Escribiendo resumen hídrico Gold en: {output_gold}")

# 2. Leer capa Silver (Parquet)
df_silver = spark.read.parquet(input_silver)

# 3. Filtrar observaciones ópticamente aptas (sin oclusión de nubes o sombras)
df_aptas = df_silver.filter(col("apta_optica") == True)

# 4. Agrupación analítica:
# Responde: ¿Cómo varía el estrés hídrico (NDMI) y presencia de agua (NDWI)
# según el distrito de riego y la técnica de manejo (AWD+MIRI vs Cascada CD)?
df_gold1 = df_aptas.groupBy(
    "distrito_riego", "municipio", "tecnica_riego", "anio", "mes"
).agg(
    count("observacion_id").alias("total_observaciones_aptas"),
    round(avg("ndmi"), 4).alias("ndmi_promedio"),
    round(avg("ndwi"), 4).alias("ndwi_promedio"),
    round(avg("ndvi"), 4).alias("ndvi_promedio"),
    round(avg("lst_c"), 2).alias("temp_superficial_c_promedio"),
    sum(when(col("ndmi") < 0.20, 1).otherwise(0)).alias("obs_en_estres_hidrico")
).withColumn(
    "pct_estres_hidrico",
    round((col("obs_en_estres_hidrico") * 100.0) / col("total_observaciones_aptas"), 2)
)

# 5. Escribir en S3 en formato Parquet
(
    df_gold1.coalesce(1)
    .write
    .mode("overwrite")
    .parquet(output_gold)
)

print("=== JOB RAMA 1 COMPLETADO EXITOSAMENTE ===")
job.commit()
