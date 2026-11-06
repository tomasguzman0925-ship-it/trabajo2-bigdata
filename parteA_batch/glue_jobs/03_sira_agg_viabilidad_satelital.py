import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, avg, sum, when, round

# 1. Obtener parámetros de entrada
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "input_silver", "output_gold_viabilidad"]
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

input_silver = args["input_silver"]
output_gold = args["output_gold_viabilidad"]

print(f"=== INICIANDO JOB RAMA 2: {args['JOB_NAME']} ===")
print(f"Leyendo capa Silver desde: {input_silver}")
print(f"Escribiendo resumen de viabilidad Gold en: {output_gold}")

# 2. Leer capa Silver (Parquet)
df_silver = spark.read.parquet(input_silver)

# 3. Agrupación analítica:
# Responde: ¿Qué porcentaje de tomas de Landsat son aprovechables sin nubes a lo largo del año
# por satélite (L8 vs L9) y municipio, y en qué meses la nubosidad exige recurrir a radar SAR?
df_gold2 = df_silver.groupBy(
    "departamento", "municipio", "satelite", "mes"
).agg(
    count("observacion_id").alias("total_tomas_programadas"),
    round(avg("nubes_escena_pct"), 2).alias("nubosidad_promedio_pct"),
    sum(when(col("apta_optica") == True, 1).otherwise(0)).alias("tomas_aptas_riego"),
    sum(when(col("apta_optica") == False, 1).otherwise(0)).alias("tomas_descartadas_nubes")
).withColumn(
    "pct_viabilidad_optica",
    round((col("tomas_aptas_riego") * 100.0) / col("total_tomas_programadas"), 2)
)

# 4. Escribir en S3 en formato Parquet
(
    df_gold2.coalesce(1)
    .write
    .mode("overwrite")
    .parquet(output_gold)
)

print("=== JOB RAMA 2 COMPLETADO EXITOSAMENTE ===")
job.commit()
