# Import libs
import findspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,when, count, desc

# Create SparkSession
sparkConf = SparkConf().setAppName("My Spark Application for IBM test")
sc = SparkContext(conf=sparkConf)
spark = SparkSession(sc)

# # # # # # # # # # # # # # # # # # # # # # # 
# PYTHON TEST
# # # # # # # # # # # # # # # # # # # # # # # 

# lectura de datos de la fuente

# Create dataframe from vuelos
vuelos_ = spark.read.load("./sources/vuelos.csv", format="csv", sep=",", inferSchema="true", header="true")
vuelos_.show()

# Create dataframe from pilots
pilots = spark.read.load("./sources/pilotos.csv", format="csv", sep=",", inferSchema="true", header="true")
pilots.show()

# Rename column
# Se renombran para un uso mas comodo en las operaciones de consulta (se eliminan los espacios)
vuelos = vuelos_.withColumnRenamed("Aerolínea", "Aerolinea")
vuelos = vuelos.withColumnRenamed("Codigo Piloto", "Codigo_Piloto")
vuelos = vuelos.withColumnRenamed("Minutos de retraso", "Minutos_de_retraso")
pilots = pilots.withColumnRenamed("Codigo Piloto", "Cod_Piloto")

# Transform columns types for vuelos
# Se cambian el tipo a integer de algunas columnas para facilitar operaciones de comparacion
vuelos = vuelos.withColumn("Codigo_Piloto", vuelos.Codigo_Piloto.cast("integer"))
vuelos = vuelos.withColumn("Aerolinea", vuelos.Aerolinea.cast("integer"))
vuelos = vuelos.withColumn("Minutos_de_retraso", vuelos.Minutos_de_retraso.cast("integer"))

# ETL steps
# Step 1 -> Add pilot name to flights df
# 
# Se ejecuta un join entre los 2 dtaframes para linkear los nombres de los pilotos
# Luego se elimina la columna duplicada que viene del join
fligths_ = vuelos.join(pilots, vuelos.Codigo_Piloto == pilots.Cod_Piloto, "left")
fligths_ = fligths_.drop("Cod_Piloto")
fligths_.show()

# Step 2 -> Remove same origin and destination
fligths_ = fligths_[fligths_["Origen"] != fligths_["Destino"]]
fligths_.show()

# Step 3 -> fill OnTime column
# Se genera un nuevo df con las columnas necesarias y modificando el valor del campo "OnTime"
# comparando el campo Minutos_de_retraso usando la funcion when de spark
# Finalmente nombramos la columna en el nuevo df como OnTime
fligths_ = fligths_.select("Codigo_Piloto", "Piloto", "Aerolinea", "Origen", "Destino", "Minutos_de_retraso", \
                    when(col("Minutos_de_retraso") <= 30, lit("A")) \
                    .when((col("Minutos_de_retraso") > 30) & (col("Minutos_de_retraso") <= 50), lit("B")) \
                    .otherwise(lit("C")) \
                    .alias("OnTime"))
fligths_.show()

# Se crean las vistas temporales para las soluciones sql
fligths_.createOrReplaceTempView("vv_vuelos")
pilots.createOrReplaceTempView("vv_pilotos")

# Export result to csv
df_pd = fligths_.toPandas()
df_pd.to_excel("results.xlsx", sheet_name='sheet_1')

# # # # # # # # # # # # # # # # # # # # # # # 
# SQL TEST
# # # # # # # # # # # # # # # # # # # # # # # 

# 1) Quien es el piloto con mas vuelos A
f_sql = spark.sql("SELECT Piloto, count(*) from vv_vuelos where OnTime = 'A' group by Piloto order by 2 desc limit 1")
f_sql.show()

# 2) Cual es la aerolinea con mas vuelos C
f2_sql = spark.sql("SELECT Aerolinea, count(*) from vv_vuelos where OnTime = 'C' group by Aerolinea order by 2 desc limit 1")
f2_sql.show()

# 3) Hung Cho vuela para las aerolineas
f3_sql = spark.sql("SELECT distinct(Piloto), Aerolinea from vv_vuelos where Piloto = 'Hung Cho'")
f3_sql.show()

# 4) Cuántos vuelos A, B, C tiene Chao Ma
f4_sql = spark.sql("SELECT Piloto, OnTime, count(*) from vv_vuelos where Piloto = 'Chao Ma' and OnTime in ('A', 'B', 'C') group by 1,2")
f4_sql.show()