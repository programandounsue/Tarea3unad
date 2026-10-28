from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("Tarea3_BigData_Batch")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

ruta = "hdfs://localhost:9000/Tarea3/rows.csv"
salida = "/home/hadoop/resultados_tarea3"

# 1. Cargar los datos
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ruta)
)

print("\n========== 1. ESQUEMA ORIGINAL ==========")
df.printSchema()

print("\n========== 2. MUESTRA ORIGINAL ==========")
df.show(10, truncate=False)

print("\n========== 3. DIMENSIONES ==========")
print("Filas originales:", df.count())
print("Columnas:", len(df.columns))

# 2. Limpieza y transformación
df = (
    df.withColumn("VALOR", F.col("VALOR").cast("double"))
      .withColumn(
          "FECHA_INICIO",
          F.to_date(F.col("VIGENCIADESDE"), "dd/MM/yyyy")
      )
      .withColumn(
          "FECHA_FIN",
          F.to_date(F.col("VIGENCIAHASTA"), "dd/MM/yyyy")
      )
)

print("\n========== 4. VALORES NULOS ==========")
df.select([
    F.count(
        F.when(F.col(c).isNull(), c)
    ).alias(c)
    for c in df.columns
]).show(truncate=False)

# Eliminar registros sin valor o sin fechas válidas
limpio = df.filter(
    F.col("VALOR").isNotNull()
    & F.col("FECHA_INICIO").isNotNull()
    & F.col("FECHA_FIN").isNotNull()
    & (F.col("VALOR") > 0)
)

print("\n========== 5. LIMPIEZA ==========")
print("Registros válidos:", limpio.count())
print("Registros descartados:", df.count() - limpio.count())

# 3. Análisis estadístico
print("\n========== 6. ESTADÍSTICAS ==========")
limpio.select(
    F.count("*").alias("registros"),
    F.round(F.min("VALOR"), 2).alias("valor_minimo"),
    F.round(F.max("VALOR"), 2).alias("valor_maximo"),
    F.round(F.avg("VALOR"), 2).alias("promedio"),
    F.round(F.stddev("VALOR"), 2).alias("desviacion_estandar")
).show(truncate=False)

print("\n========== 7. VALORES MAYORES A 5000 ==========")
limpio.filter(F.col("VALOR") > 5000) \
    .select("VALOR", "FECHA_INICIO", "FECHA_FIN") \
    .orderBy(F.desc("VALOR")) \
    .show(20, truncate=False)

print("\n========== 8. DIEZ VALORES MÁS ALTOS ==========")
limpio.orderBy(F.desc("VALOR")) \
    .select("VALOR", "FECHA_INICIO", "FECHA_FIN") \
    .show(10, truncate=False)

# 4. Análisis temporal por mes
mensual = (
    limpio.withColumn(
        "MES",
        F.date_format("FECHA_INICIO", "yyyy-MM")
    )
    .groupBy("MES")
    .agg(
        F.count("*").alias("cantidad_registros"),
        F.round(F.avg("VALOR"), 2).alias("promedio"),
        F.round(F.min("VALOR"), 2).alias("minimo"),
        F.round(F.max("VALOR"), 2).alias("maximo")
    )
    .orderBy("MES")
)

print("\n========== 9. RESUMEN MENSUAL ==========")
mensual.show(100, truncate=False)

# 5. Guardar resultados
mensual.write.mode("overwrite").option(
    "header", True
).csv(salida + "/resumen_mensual")

limpio.write.mode("overwrite").option(
    "header", True
).csv(salida + "/datos_limpios")

# 6. Gráficas a partir del resumen mensual
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    datos = mensual.toPandas()

    if len(datos) > 0:
        plt.figure(figsize=(12, 5))
        plt.plot(datos["MES"], datos["promedio"], marker="o")
        plt.title("Promedio mensual de VALOR")
        plt.xlabel("Mes")
        plt.ylabel("Valor promedio (COP)")
        plt.xticks(rotation=60)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(salida + "/promedio_mensual.png", dpi=150)
        plt.close()

        plt.figure(figsize=(12, 5))
        plt.bar(datos["MES"], datos["cantidad_registros"])
        plt.title("Cantidad de registros por mes")
        plt.xlabel("Mes")
        plt.ylabel("Número de registros")
        plt.xticks(rotation=60)
        plt.tight_layout()
        plt.savefig(salida + "/registros_mensuales.png", dpi=150)
        plt.close()

        print("\nGráficas guardadas en:", salida)
except ImportError:
    print("\nNo se encontró matplotlib. El análisis y los CSV se generaron.")
    print("Podremos instalar o configurar la visualización después.")

print("\nResultados guardados en:", salida)
print("PROCESAMIENTO BATCH FINALIZADO")

spark.stop()
