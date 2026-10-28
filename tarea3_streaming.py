from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, min, max, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = (
    SparkSession.builder
    .appName("Tarea3Streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

esquema = StructType([
    StructField("VALOR", DoubleType(), True),
    StructField("UNIDAD", StringType(), True),
    StructField("VIGENCIADESDE", StringType(), True),
    StructField("VIGENCIAHASTA", StringType(), True)
])

datos = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "datos_tarea3")
    .option("startingOffsets", "latest")
    .load()
)

json_datos = datos.select(
    from_json(col("value").cast("string"), esquema).alias("dato")
).select("dato.*")

validos = json_datos.filter(
    col("VALOR").isNotNull() & col("UNIDAD").isNotNull()
)

print("SPARK STREAMING: calculando estadisticas por ventana")

estadisticas = validos.groupBy(
    window(to_timestamp(col("VIGENCIADESDE"), "dd/MM/yyyy"), "1 minute"),
    col("UNIDAD")
).agg(
    count("*").alias("cantidad_registros"),
    avg("VALOR").alias("promedio"),
    min("VALOR").alias("minimo"),
    max("VALOR").alias("maximo")
)

consulta = (
    estadisticas.writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/checkpoint_tarea3")
    .start()
)

consulta.awaitTermination()
